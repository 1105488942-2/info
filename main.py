#!/usr/bin/env python3
"""B站开播监听 → QQ群通知（内置网页管理）

轮询 B 站直播间状态，检测到开播时通过 OneBot v11 接口
（NapCat / LLOneBot）向绑定的 QQ 群发送消息。

订阅关系（直播间 ↔ QQ群）保存在 subscriptions.json，
通过网页管理界面增删改，保存后下一轮轮询自动生效。

用法：
    python3 main.py              # 正常运行（含网页管理）
    python3 main.py --test       # 向所有订阅群发一条测试消息
    python3 main.py -c a.toml    # 指定配置文件

依赖：requests
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hmac
import json
import logging
import signal
import threading
import time
import tomllib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import subprocess

import requests

BASE_DIR = Path(__file__).resolve().parent
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
ROOM_INFO_API = "https://api.live.bilibili.com/room/v1/Room/get_info"
ANCHOR_API = "https://api.live.bilibili.com/live_user/v1/UserInfo/get_anchor_in_room"
LIVE_URL = "https://live.bilibili.com/{room_id}"
STATUS_NAMES = {0: "未开播", 1: "直播中", 2: "轮播"}
DEFAULT_TEMPLATE = "{name} 开播啦！\n{title}\n{url}"

log = logging.getLogger("live-notify")

# 轮询状态快照，供网页展示
STATUS_LOCK = threading.Lock()
LAST_POLL: dict = {"ts": 0, "ok": None, "error": "", "rooms": {}}
NAME_CACHE: dict[int, str] = {}

# 登录失败防爆破：同一 IP 10 分钟内失败 5 次，封禁 30 分钟
AUTH_LOCK = threading.Lock()
AUTH_FAILS: dict[str, list[float]] = {}
AUTH_BLOCK: dict[str, float] = {}
MAX_FAILS = 5
FAIL_WINDOW = 600
BLOCK_SECONDS = 1800

# QQ 私聊查询：OneBot 事件上报路径、默认触发指令、回复冷却（秒）
EVENT_PATH = "/onebot/event"
DEFAULT_QUERY_COMMANDS = ("查询", "状态", "配置", "config", "status")
QUERY_COOLDOWN_SECONDS = 3
QUERY_COOLDOWN: dict[int, float] = {}
PENDING_REBOOT: dict[int, float] = {}  # uid -> 请求重启服务器的时间戳
WS_CONNECTED = threading.Event()  # NapCat WS 事件通道是否已连通

# 全局 QQ 官方机器人客户端（运行时初始化）
QQ_CLIENT: QQOfficialClient | None = None


def auth_block_remaining(ip: str) -> int:
    """返回该 IP 剩余封禁秒数，0 表示未封禁。"""
    now = time.time()
    with AUTH_LOCK:
        until = AUTH_BLOCK.get(ip, 0)
        if until <= now:
            AUTH_BLOCK.pop(ip, None)
            return 0
        return int(until - now)


def record_auth_failure(ip: str) -> None:
    now = time.time()
    with AUTH_LOCK:
        fails = [t for t in AUTH_FAILS.get(ip, []) if now - t < FAIL_WINDOW]
        fails.append(now)
        if len(fails) >= MAX_FAILS:
            AUTH_BLOCK[ip] = now + BLOCK_SECONDS
            AUTH_FAILS.pop(ip, None)
            log.warning("管理页登录连续失败，封禁 IP %s 30 分钟", ip)
        else:
            AUTH_FAILS[ip] = fails
            log.warning("管理页登录失败（IP %s，%d/%d）", ip, len(fails), MAX_FAILS)
        if len(AUTH_FAILS) > 5000:  # 防止异常流量撑大内存
            cutoff = now - FAIL_WINDOW
            for key in [k for k, v in AUTH_FAILS.items() if not v or v[-1] < cutoff]:
                AUTH_FAILS.pop(key, None)


def clear_auth_failures(ip: str) -> None:
    with AUTH_LOCK:
        AUTH_FAILS.pop(ip, None)


# ============================================================
# QQ 官方机器人客户端（合规、不封号）
# ============================================================
class QQOfficialClient:
    """QQ 官方机器人 OpenAPI 客户端 + WebSocket 事件监听。

    负责：token 自动刷新、发送群/私聊消息、维护 group_openid 映射、
    WebSocket 网关连接（接收事件以自动填充 group_openid）。
    """

    BASE_URL = "https://api.bot.qq.com"
    WS_INTENTS = 1 << 25  # GROUP_AND_C2C_EVENT

    def __init__(self, appid: str, secret: str, group_map_path: Path):
        self.appid = str(appid)
        self.secret = str(secret)
        self.access_token: str | None = None
        self.token_expires: float = 0  # unix 秒，提前 60 秒刷新
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})
        self.group_map_path = group_map_path
        # {group_id_str: group_openid} — 订阅里用的是数字群号，官方 API 要 openid
        self.group_map: dict[str, str] = self._load_group_map()
        self.user_openid_map: dict[str, str] = {}  # {qq_str: user_openid}  for 私聊
        self.ws_connected = threading.Event()
        self._ws_seq: int | None = None
        self._ws_session_id: str | None = None

    # ---- group_openid 映射 ----
    def _load_group_map(self) -> dict[str, str]:
        try:
            data = json.loads(self.group_map_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return {str(k): str(v) for k, v in data.items()}
        except (FileNotFoundError, ValueError, OSError):
            pass
        return {}

    def _save_group_map(self) -> None:
        try:
            tmp = self.group_map_path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(self.group_map, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(self.group_map_path)
        except OSError as exc:
            log.warning("group_map 写入失败：%s", exc)

    def register_group_openid(self, group_openid: str, numeric_hint: str | None = None) -> None:
        """收到群相关事件时调用，把 openid 记录下来。

        numeric_hint：用户可能在私聊指令里告诉我们"这个 openid 对应群号 668286732"
        """
        group_openid = str(group_openid)
        # 先看 openid 本身是不是数字——官方 openid 是 32 位十六进制，数字群号 ≤ 10 位
        if len(group_openid) <= 10 and group_openid.isdigit():
            # 这本身就是数字群号（不太可能，但防御一下）
            return
        # 反查：如果我们还没存任何数字→openid，先存一个占位
        # 真正的映射需要用户告诉我们或从别处拿到
        if numeric_hint and numeric_hint.isdigit():
            key = str(numeric_hint)
            if self.group_map.get(key) != group_openid:
                self.group_map[key] = group_openid
                self._save_group_map()
                log.info("群映射 %s → %s（openid）", key, group_openid)
        # 同时存 openid → 未知数字群号的反向提示
        log.info("收到 group_openid=%s，请在管理页或私聊用「绑定群 %s <群号>」关联",
                 group_openid, group_openid)

    def get_group_openid(self, group_id: int) -> str | None:
        return self.group_map.get(str(group_id))

    # ---- token 管理 ----
    def get_token(self) -> str:
        """返回有效的 access_token，过期自动刷新。"""
        now = time.time()
        if self.access_token and self.token_expires - 60 > now:
            return self.access_token
        try:
            resp = self.session.post(
                f"{self.BASE_URL}/app/getAppAccessToken",
                json={"appId": self.appid, "clientSecret": self.secret},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            token = data.get("access_token")
            expires = int(data.get("expires_in", 7200))
            if not token:
                raise RuntimeError(f"获取 token 失败：{data}")
            self.access_token = token
            self.token_expires = now + expires
            log.info("QQ 官方 token 已刷新（有效期 %d 秒）", expires)
            return self.access_token
        except Exception as exc:
            raise RuntimeError(f"获取 QQ token 失败：{exc}") from exc

    def _headers(self) -> dict:
        return {"Authorization": f"QQBot {self.get_token()}"}

    # ---- API 调用 ----
    def _post(self, path: str, body: dict) -> dict:
        url = f"{self.BASE_URL}{path}"
        token = self.get_token()
        try:
            resp = self.session.post(url, json=body, headers=self._headers(), timeout=15)
        except requests.RequestException as exc:
            raise RuntimeError(f"QQ 官方 API 连接失败 {path}：{exc}") from exc
        if resp.status_code == 401:
            # token 过期，强制刷新后重试一次
            self.access_token = None
            token = self.get_token()
            resp = self.session.post(url, json=body, headers=self._headers(), timeout=15)
        if resp.status_code != 200:
            raise RuntimeError(f"QQ API {path} 返回 HTTP {resp.status_code}: {resp.text[:200]}")
        try:
            payload = resp.json()
        except ValueError:
            raise RuntimeError(f"QQ API 返回非 JSON：{resp.text[:200]}")
        # 官方 API 成功时 payload 不含 code 字段（或 code=0）；失败时会有
        if payload.get("code", 0) != 0:
            raise RuntimeError(f"QQ API 错误 code={payload.get('code')} msg={payload.get('message')}")
        return payload

    def send_group_message(self, group_openid: str, content: str, msg_type: int = 0,
                           event_id: str | None = None, msg_id: str | None = None) -> None:
        """向群发送消息。

        主动推送：不传 event_id 和 msg_id
        被动回复（回复 @机器人 的消息）：传 event_id（事件 ID）或 msg_id（消息 ID），必须在有效期内（5分钟）
        """
        body: dict = {"msg_type": msg_type, "content": content}
        if event_id:
            body["event_id"] = event_id
        elif msg_id:
            body["msg_id"] = msg_id
        self._post(f"/v2/groups/{group_openid}/messages", body)

    def upload_group_image(self, group_openid: str, image_url: str) -> str:
        """上传图片到群文件服务，返回 file_info（用于发富媒体消息）。

        image_url 必须是公网可访问的 http(s) URL，腾讯会自己拉取。
        返回的 file_info 有效期由 ttl 决定（通常 86400 秒）。
        """
        body = {"file_type": 1, "url": image_url}  # 1 = 图片(png/jpg)
        resp = self._post(f"/v2/groups/{group_openid}/files", body)
        file_info = resp.get("file_info")
        if not file_info:
            raise RuntimeError(f"图片上传未返回 file_info: {resp}")
        return file_info

    def send_group_with_image(self, group_openid: str, content: str, image_url: str) -> None:
        """向群发带图片的富媒体消息（先上传图片，再发送）。"""
        file_info = self.upload_group_image(group_openid, image_url)
        body = {
            "msg_type": 7,  # 富媒体
            "content": content,
            "media": {"file_info": file_info},
        }
        self._post(f"/v2/groups/{group_openid}/messages", body)

    def send_private_message(self, user_openid: str, content: str, msg_type: int = 0,
                             event_id: str | None = None, msg_id: str | None = None) -> None:
        """向用户发送单聊消息。"""
        body: dict = {"msg_type": msg_type, "content": content}
        if event_id:
            body["event_id"] = event_id
        elif msg_id:
            body["msg_id"] = msg_id
        self._post(f"/v2/users/{user_openid}/messages", body)

    # ---- WebSocket 网关 ----
    async def _connect_gateway(self) -> None:
        """连接 QQ 官方 WebSocket 网关，接收事件。"""
        import websockets

        # 获取网关地址
        try:
            resp = self.session.get(
                f"{self.BASE_URL}/gateway/bot", headers=self._headers(), timeout=10
            )
            resp.raise_for_status()
            gateway_url = resp.json().get("url")
        except Exception as exc:
            log.error("获取网关地址失败：%s", exc)
            return

        while True:
            try:
                async with websockets.connect(gateway_url, ping_interval=None) as ws:
                    log.info("已连接 QQ 官方网关 %s", gateway_url)
                    self.ws_connected.set()

                    # Hello
                    hello = json.loads(await ws.recv())
                    hb_interval = hello["d"]["heartbeat_interval"]

                    # Identify / Resume
                    if self._ws_session_id and self._ws_seq is not None:
                        # Resume
                        payload = {
                            "op": 6,
                            "d": {
                                "token": f"QQBot {self.get_token()}",
                                "session_id": self._ws_session_id,
                                "seq": self._ws_seq,
                            },
                        }
                        await ws.send(json.dumps(payload))
                        log.info("发送 Resume")
                    else:
                        payload = {
                            "op": 2,
                            "d": {
                                "token": f"QQBot {self.get_token()}",
                                "intents": self.WS_INTENTS,
                                "shard": [0, 1],
                                "properties": {"$os": "linux", "$browser": "python", "$device": "python"},
                            },
                        }
                        await ws.send(json.dumps(payload))
                        log.info("发送 Identify")

                    # Heartbeat loop
                    seq_holder = [None]

                    async def _heartbeat() -> None:
                        await asyncio.sleep(hb_interval / 1000)
                        while True:
                            await ws.send(json.dumps({"op": 1, "d": seq_holder[0]}))
                            await asyncio.sleep(hb_interval / 1000)

                    hb_task = asyncio.create_task(_heartbeat())

                    try:
                        async for raw in ws:
                            msg = json.loads(raw)
                            op = msg.get("op")
                            seq = msg.get("s")
                            if seq is not None:
                                self._ws_seq = seq
                                seq_holder[0] = seq

                            if op == 0:  # Dispatch
                                self._handle_event(msg.get("t"), msg.get("d"))
                            elif op == 11:  # Heartbeat ACK
                                pass
                            elif op == 7:  # Reconnect
                                log.warning("网关要求重连")
                                break
                            elif op == 9:  # Invalid Session
                                log.warning("WS Session 无效，重置后重连")
                                self._ws_session_id = None
                                self._ws_seq = None
                                break
                            elif op == 10:
                                pass  # Already handled Hello
                            elif op == 1:
                                pass  # Server heartbeat
                            else:
                                log.debug("WS op=%s t=%s", op, msg.get("t"))

                    finally:
                        hb_task.cancel()
                        self.ws_connected.clear()
            except Exception as exc:
                log.warning("QQ 网关断开（%s），5 秒后重连", exc)
                await asyncio.sleep(5)

    def _handle_event(self, event_name: str, data: dict | None) -> None:
        """处理 QQ 官方网关事件。"""
        if event_name == "READY" and isinstance(data, dict):
            self._ws_session_id = data.get("session_id")
            log.info("QQ 官方机器人 READY，session=%s", self._ws_session_id)
            return

        if data is None:
            return

        group_openid = data.get("group_openid") if isinstance(data, dict) else None
        if group_openid:
            self.register_group_openid(group_openid)

        if event_name == "GROUP_ADD_ROBOT":
            log.info("机器人加入群 group_openid=%s", group_openid)

        elif event_name == "GROUP_AT_MESSAGE_CREATE":
            content = str(data.get("content") or "").strip()
            author = data.get("author") or {}
            user_openid = author.get("user_openid") or author.get("member_openid") or author.get("id")
            msg_id = data.get("id")  # 可用于被动回复
            self._try_handle_group_query(content, user_openid, group_openid,
                                         event_id=msg_id, msg_id=msg_id)

        elif event_name == "GROUP_MESSAGE_CREATE":
            # 全量群消息（需要群主授权）
            content = str(data.get("content") or "").strip()
            author = data.get("author") or {}
            if author.get("bot"):
                return
            user_openid = author.get("user_openid") or author.get("member_openid") or author.get("id")
            msg_id = data.get("id")
            self._try_handle_group_query(content, user_openid, group_openid,
                                         event_id=msg_id, msg_id=msg_id)

        elif event_name == "C2C_MESSAGE_CREATE":
            content = str(data.get("content") or "").strip()
            author = data.get("author") or {}
            user_openid = author.get("user_openid") or author.get("id")
            msg_id = data.get("id")
            log.info("收到单聊 user_openid=%s content=%r", user_openid, content[:40])
            self._try_handle_private_query(content, user_openid, msg_id=msg_id)

    def _try_handle_group_query(self, content: str, user_openid: str | None, group_openid: str | None,
                                event_id: str | None = None, msg_id: str | None = None) -> None:
        """@机器人时的查询指令处理。"""
        if not content or not group_openid:
            return
        query_commands = {"查询", "状态", "配置", "config", "status", "help", "帮助", "?", "？"}
        cmd = content.strip().lower()
        if cmd not in {c.lower() for c in query_commands} and not cmd.startswith(("绑定群", "group")):
            return
        log.info("群指令：%s（from %s in %s）", content[:40], user_openid, group_openid)
        reply_lines = ["【QQ官方机器人 · 群映射状态】"]
        reply_lines.append(f"已知 {len(self.group_map)} 个群映射：")
        for gid, oid in self.group_map.items():
            reply_lines.append(f"  群号 {gid} → openid {oid[:8]}...")
        reply_lines.append("请发：绑定群 <openid> <数字群号> 来关联")
        try:
            # 优先用被动回复（不需要群主开主动推送开关）
            if msg_id:
                self.send_group_message(group_openid, "\n".join(reply_lines), msg_id=msg_id)
            else:
                self.send_group_message(group_openid, "\n".join(reply_lines), event_id=event_id)
        except Exception as exc:
            log.error("回复群指令失败：%s", exc)

    def _try_handle_private_query(self, content: str, user_openid: str | None,
                                  msg_id: str | None = None) -> None:
        """私聊查询指令处理。"""
        if not content or not user_openid:
            return
        cmd = content.strip().lower()
        if cmd in {"帮助", "help", "?", "？"}:
            reply = (
                "可用指令：\n"
                "查询 —— 查看当前群映射状态\n"
                "绑定群 <openid> <群号> —— 手动关联 openid 和数字群号\n"
                "（群内 @机器人 也能看到状态）"
            )
        elif cmd in {"查询", "状态", "配置", "config", "status"}:
            reply_lines = ["【群映射状态】"]
            if not self.group_map:
                reply_lines.append("（暂无映射）")
            else:
                for gid, oid in self.group_map.items():
                    reply_lines.append(f"  群号 {gid} → openid {oid[:8]}...")
            reply_lines.append("群内 @机器人 发送指令后，在此回复确认并绑定群号")
            reply = "\n".join(reply_lines)
        elif cmd.startswith(("绑定群", "group")):
            parts = cmd.split()
            if len(parts) >= 3:
                oid = parts[1]
                numeric = parts[2]
                if numeric.isdigit():
                    self.group_map[numeric] = oid
                    self._save_group_map()
                    reply = f"已绑定：群号 {numeric} → openid {oid[:8]}..."
                else:
                    reply = "用法：绑定群 <openid> <数字群号>"
            else:
                reply = "用法：绑定群 <openid> <数字群号>"
        else:
            reply = "可用指令：查询 / 帮助 / 绑定群 <openid> <群号>"
        try:
            kwargs: dict = {}
            if msg_id:
                kwargs["msg_id"] = msg_id
            self.send_private_message(user_openid, reply, **kwargs)
        except Exception as exc:
            log.error("私聊回复失败：%s", exc)

    def start(self) -> None:
        """后台线程：连接 WebSocket 网关。"""
        import asyncio
        threading.Thread(
            target=lambda: asyncio.run(self._connect_gateway()),
            name="qq-official-ws",
            daemon=True,
        ).start()


def load_config(path: Path) -> dict:
    """读取并校验配置，出错直接退出并给出可读提示。"""
    if not path.exists():
        raise SystemExit(f"找不到配置文件：{path}")
    try:
        with path.open("rb") as f:
            cfg = tomllib.load(f)
    except tomllib.TOMLDecodeError as exc:
        raise SystemExit(f"配置文件格式错误：{exc}")

    # 环境变量覆盖（GitHub Actions / Docker Secrets 注入）
    qq_official = cfg.setdefault("qq_official", {})
    if os.environ.get("QQ_APPID"):
        qq_official["appid"] = os.environ["QQ_APPID"]
    if os.environ.get("QQ_SECRET"):
        qq_official["secret"] = os.environ["QQ_SECRET"]
    if os.environ.get("QQ_OFFICIAL_ENABLED"):
        qq_official["enabled"] = os.environ["QQ_OFFICIAL_ENABLED"].lower() in ("1", "true", "yes")
    if qq_official.get("enabled"):
        if not str(qq_official.get("appid") or "").strip() or not str(qq_official.get("secret") or "").strip():
            raise SystemExit("qq_official.enabled=true 时必须提供 appid 和 secret")

    qq = cfg.setdefault("qq", {})
    # 如果没启用官方机器人，OneBot 配置必须存在
    if not qq_official.get("enabled"):
        if not str(qq.get("api_base") or "").strip():
            raise SystemExit("未启用 qq_official 时，配置缺少 qq.api_base")
    bili = cfg.setdefault("bilibili", {})
    bili["poll_interval"] = max(int(bili.get("poll_interval", 60)), 10)

    query = cfg.setdefault("query", {})
    allowed = query.get("allowed_qq") or []
    if not isinstance(allowed, list):
        raise SystemExit("配置 query.allowed_qq 必须是 QQ 号数组")
    try:
        query["allowed_qq"] = [int(q) for q in allowed]
    except (TypeError, ValueError):
        raise SystemExit("配置 query.allowed_qq 里的 QQ 号必须是数字")
    commands = query.get("commands") or list(DEFAULT_QUERY_COMMANDS)
    query["commands"] = [str(c).strip().lower() for c in commands if str(c).strip()]
    return cfg


def load_subscriptions(path: Path) -> list[dict]:
    """读取订阅列表；文件不存在或损坏时返回空列表。"""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return []
    except (ValueError, OSError) as exc:
        log.warning("订阅文件读取失败：%s", exc)
        return []
    try:
        return normalize_subscriptions(raw)
    except ValueError as exc:
        log.warning("订阅文件内容不合法：%s", exc)
        return []


def normalize_subscriptions(raw) -> list[dict]:
    """校验并规范化订阅列表，剔除重复项。"""
    if isinstance(raw, dict):
        raw = raw.get("subscriptions")
    if not isinstance(raw, list):
        raise ValueError("订阅数据必须是数组")
    result: list[dict] = []
    seen: set[tuple[int, int]] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        try:
            room_id = int(item.get("room_id"))
            group_id = int(item.get("group_id"))
        except (TypeError, ValueError):
            raise ValueError("room_id / group_id 必须是数字")
        if room_id <= 0 or group_id <= 0:
            raise ValueError("room_id / group_id 必须是正整数")
        pair = (room_id, group_id)
        if pair in seen:
            continue
        seen.add(pair)
        result.append(
            {
                "room_id": room_id,
                "group_id": group_id,
                "enabled": bool(item.get("enabled", True)),
                "note": str(item.get("note") or "")[:40],
            }
        )
    if len(result) > 100:
        raise ValueError("订阅数量不能超过 100 条")
    return result


def save_subscriptions(path: Path, subs: list[dict]) -> None:
    with STATUS_LOCK:
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps(subs, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)


def bili_get(session: requests.Session, url: str, params: dict) -> dict:
    resp = session.get(url, params=params, timeout=10)
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("code") != 0:
        msg = payload.get("msg") or payload.get("message") or ""
        raise RuntimeError(f"接口返回 code={payload.get('code')} msg={msg}")
    return payload.get("data") or {}


def fetch_anchor_name(session: requests.Session, room_id: int) -> str:
    cached = NAME_CACHE.get(room_id)
    if cached:
        return cached
    try:
        data = bili_get(session, ANCHOR_API, {"roomid": room_id})
        name = (data.get("info") or {}).get("uname") or str(room_id)
    except Exception as exc:
        log.warning("获取主播昵称失败，将用房间号代替：%s", exc)
        return str(room_id)
    NAME_CACHE[room_id] = name
    return name


def build_message(cfg: dict, info: dict, name: str, room_id: int) -> list[dict]:
    """拼装 OneBot 消息段：可选 @全体 + 文本 + 封面图。"""
    notify_cfg = cfg.get("notify") or {}
    values = {
        "name": name,
        "title": (info.get("title") or "无标题").strip(),
        "room_id": room_id,
        "url": LIVE_URL.format(room_id=room_id),
        "time": info.get("live_time") or "",
    }

    template = notify_cfg.get("message_template") or DEFAULT_TEMPLATE
    try:
        text = template.format(**values)
    except (KeyError, IndexError, ValueError) as exc:
        log.warning("消息模板解析失败（%s），改用默认模板", exc)
        text = DEFAULT_TEMPLATE.format(**values)

    segments: list[dict] = []
    if notify_cfg.get("at_all"):
        segments.append({"type": "at", "data": {"qq": "all"}})
        segments.append({"type": "text", "data": {"text": " "}})
    segments.append({"type": "text", "data": {"text": text}})

    cover = info.get("keyframe") or info.get("user_cover") or ""
    if notify_cfg.get("include_cover", True) and cover.startswith("http"):
        segments.append({"type": "image", "data": {"file": cover}})
    return segments


def build_message_text(cfg: dict, info: dict, name: str, room_id: int, include_cover: bool | None = None) -> str:
    """拼装纯文本消息（QQ 官方机器人用，不支持 OneBot 消息段格式）。

    include_cover=False 时不在文本里追加封面 URL（用于富媒体消息，封面通过 media 字段发送）；
    include_cover=None（默认）时遵循配置里的 include_cover 开关。
    """
    notify_cfg = cfg.get("notify") or {}
    values = {
        "name": name,
        "title": (info.get("title") or "无标题").strip(),
        "room_id": room_id,
        "url": LIVE_URL.format(room_id=room_id),
        "time": info.get("live_time") or "",
    }
    template = notify_cfg.get("message_template") or DEFAULT_TEMPLATE
    try:
        text = template.format(**values)
    except (KeyError, IndexError, ValueError):
        text = DEFAULT_TEMPLATE.format(**values)
    if notify_cfg.get("at_all"):
        text = "@全体成员 " + text  # 官方机器人不支持真正的 @全体，只是文本提及
    if include_cover is None:
        include_cover = bool(notify_cfg.get("include_cover", True))
    if include_cover:
        cover = info.get("keyframe") or info.get("user_cover") or ""
        if cover.startswith("http"):
            text += "\n封面：" + cover
    return text


def call_onebot(session: requests.Session, cfg: dict, api: str, body: dict) -> None:
    """调用 OneBot v11 HTTP 接口，失败抛异常。"""
    qq_cfg = cfg["qq"]
    url = str(qq_cfg.get("api_base") or "http://127.0.0.1:3000").rstrip("/") + "/" + api

    headers = {}
    token = str(qq_cfg.get("access_token") or "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        resp = session.post(url, json=body, headers=headers, timeout=15)
    except requests.RequestException as exc:
        raise RuntimeError(f"无法连接 OneBot 服务 {url}：{exc}") from exc

    if resp.status_code == 401:
        raise RuntimeError("OneBot 拒绝了请求，请检查 access_token 是否与 NapCat 配置一致")
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
    try:
        payload = resp.json()
    except ValueError:
        raise RuntimeError(f"返回内容不是 JSON：{resp.text[:200]}")
    if payload.get("retcode") != 0:
        detail = payload.get("message") or payload.get("wording") or ""
        raise RuntimeError(f"发送失败 retcode={payload.get('retcode')} {detail}")


def _use_qq_official(cfg: dict) -> bool:
    return bool((cfg.get("qq_official") or {}).get("enabled"))


def send_group_message(session: requests.Session, cfg: dict, group_id: int,
                       segments: list[dict] | None = None,
                       text_only: str | None = None) -> None:
    """发送群消息。自动选择 QQ 官方机器人或 OneBot 路径。

    - segments: OneBot v11 消息段列表（给 OneBot 用）
    - text_only: 纯文本（给 QQ 官方机器人用）；如未提供则从 segments 提取文本
    """
    if _use_qq_official(cfg):
        global QQ_CLIENT
        if QQ_CLIENT is None:
            raise RuntimeError("QQ 官方机器人客户端未初始化")
        openid = QQ_CLIENT.get_group_openid(group_id)
        if not openid:
            raise RuntimeError(f"群号 {group_id} 未绑定 group_openid，请先在群里 @机器人 或私聊发送「绑定群」指令")
        content = text_only if text_only is not None else _segments_to_text(segments or [])
        QQ_CLIENT.send_group_message(openid, content)
    else:
        call_onebot(session, cfg, "send_group_msg", {"group_id": group_id, "message": segments or []})


def send_group_with_image(session: requests.Session, cfg: dict, group_id: int,
                          text_only: str, image_url: str) -> None:
    """向群发带图片的富媒体消息（QQ 官方机器人路径，走先上传再发送两步）。"""
    global QQ_CLIENT
    if QQ_CLIENT is None:
        raise RuntimeError("QQ 官方机器人客户端未初始化")
    openid = QQ_CLIENT.get_group_openid(group_id)
    if not openid:
        raise RuntimeError(f"群号 {group_id} 未绑定 group_openid，请先在群里 @机器人 或私聊发送「绑定群」指令")
    QQ_CLIENT.send_group_with_image(openid, text_only, image_url)


def send_private_message(session: requests.Session, cfg: dict, user_id: int | str,
                         segments: list[dict] | None = None,
                         text_only: str | None = None) -> None:
    """发送私聊消息。自动选择 QQ 官方机器人或 OneBot 路径。"""
    if _use_qq_official(cfg):
        global QQ_CLIENT
        if QQ_CLIENT is None:
            raise RuntimeError("QQ 官方机器人客户端未初始化")
        # 官方机器人私聊用 user_openid，不是数字 QQ 号
        openid = QQ_CLIENT.user_openid_map.get(str(user_id)) or QQ_CLIENT.user_openid_map.get(str(user_id), str(user_id))
        # 临时：如果 user_id 本身就是 openid（32位），直接用
        sid = str(user_id)
        if len(sid) == 32 and not sid.isdigit():
            openid = sid
        content = text_only if text_only is not None else _segments_to_text(segments or [])
        QQ_CLIENT.send_private_message(openid, content)
    else:
        call_onebot(session, cfg, "send_private_msg", {"user_id": user_id, "message": segments or []})


def _segments_to_text(segments: list[dict]) -> str:
    """把 OneBot 消息段列表转成纯文本（QQ 官方机器人只支持 text/image 等少数类型）。"""
    parts: list[str] = []
    for seg in segments:
        if not isinstance(seg, dict):
            continue
        t = seg.get("type", "")
        data = seg.get("data") or {}
        if t == "text":
            parts.append(str(data.get("text", "")))
        elif t == "at" and data.get("qq") == "all":
            parts.append("@全体成员")
        elif t == "at":
            parts.append(f"@{data.get('qq', '')}")
        elif t == "image":
            url = str(data.get("file", ""))
            if url.startswith("http"):
                parts.append("\n" + url)
    return "".join(parts)


def notify(session: requests.Session, cfg: dict, info: dict, name: str, room_id: int, group_id: int) -> bool:
    """发送开播通知，最多重试 3 次。返回是否成功。"""
    is_official = _use_qq_official(cfg)
    notify_cfg = cfg.get("notify") or {}
    cover = info.get("keyframe") or info.get("user_cover") or ""
    include_cover = bool(notify_cfg.get("include_cover", True)) and cover.startswith("http")

    for attempt in range(1, 4):
        try:
            if is_official:
                text = build_message_text(cfg, info, name, room_id, include_cover=False)
                if include_cover:
                    send_group_with_image(session, cfg, group_id, text, cover)
                else:
                    send_group_message(session, cfg, group_id, text_only=text)
            else:
                segments = build_message(cfg, info, name, room_id)
                send_group_message(session, cfg, group_id, segments=segments)
            log.info("已发送开播通知：房间 %s → QQ 群 %s", room_id, group_id)
            return True
        except Exception as exc:
            log.error("发送通知失败（第 %d/3 次，群 %s）：%s", attempt, group_id, exc)
            if attempt < 3:
                time.sleep(3)
    return False


def build_status_text(cfg: dict, sub_path: Path) -> str:
    """拼装当前配置与运行状态文本，供私聊查询回复。"""
    with STATUS_LOCK:
        snap = dict(LAST_POLL)
    subs = load_subscriptions(sub_path)
    notify_cfg = cfg.get("notify") or {}
    interval = int(cfg["bilibili"]["poll_interval"])

    lines = ["【B站开播通知 · 当前配置】"]
    if snap.get("ts"):
        ts = time.strftime("%m-%d %H:%M:%S", time.localtime(int(snap["ts"])))
        if snap.get("ok") is False:
            lines.append(f"轮询异常：{snap.get('error') or '未知错误'}（上次 {ts}）")
        else:
            lines.append(f"轮询正常 · 每 {interval} 秒 · 上次 {ts}")
    else:
        lines.append(f"尚未开始轮询 · 每 {interval} 秒")

    at_all = "开" if notify_cfg.get("at_all") else "关"
    cover = "开" if notify_cfg.get("include_cover", True) else "关"
    lines.append(f"@全体 {at_all} · 封面图 {cover}")
    template = str(notify_cfg.get("message_template") or DEFAULT_TEMPLATE).replace("\n", "\\n")
    lines.append(f"模板：{template}")

    rooms = snap.get("rooms") or {}
    lines.append(f"订阅 {len(subs)} 条：")
    if not subs:
        lines.append("（暂无订阅，可到管理页添加）")
    for i, sub in enumerate(subs, 1):
        room = rooms.get(str(sub["room_id"])) or {}
        status = STATUS_NAMES.get(room.get("live_status"), "未知")
        name = sub.get("note") or room.get("name") or ""
        name_part = f"（{name}）" if name else ""
        enabled = "启用" if sub.get("enabled") else "停用"
        lines.append(f"{i}. 房间 {sub['room_id']}{name_part} → 群 {sub['group_id']}〔{enabled}〕{status}")
    return "\n".join(lines)


HELP_TEXT = (
    "可用指令（空格分隔）：\n"
    "查询 —— 查看当前配置\n"
    "服务器 —— 查看服务器状态\n"
    "添加 房间号 群号 [备注]\n"
    "删除/启用/停用 房间号 群号 —— 也可直接用查询结果里的序号，如：停用 2\n"
    "@全体 开 / @全体 关\n"
    "封面 开 / 封面 关\n"
    "模板 内容 —— 占位符 {name}主播 {title}标题 {url}链接，\\n 表示换行\n"
    "重启 弹幕 / 面板 / napcat / 通知\n"
    "重启 服务器 —— 需二次确认\n"
    "示例：添加 1726453326 668286732"
)


def get_server_status() -> str:
    """收集服务器运行状态摘要。"""
    lines = ["【服务器状态】"]
    try:
        up = subprocess.run(["uptime", "-p"], capture_output=True, text=True, timeout=5).stdout.strip()
        up = up.replace("up ", "").replace("weeks", "周").replace("week", "周").replace(
            "days", "天").replace("day", "天").replace("hours", "小时").replace("hour", "小时").replace(
            "minutes", "分钟").replace("minute", "分钟").replace(",", "")
    except Exception:
        up = "?"
    try:
        load1, load5, load15 = open("/proc/loadavg").read().split()[:3]
        lines.append(f"运行 {up} · 负载 {load1}/{load5}/{load15}")
    except Exception:
        pass
    try:
        mem = {}
        for line in open("/proc/meminfo"):
            k = line.split(":")[0]
            if k in ("MemTotal", "MemAvailable", "SwapTotal", "SwapFree"):
                mem[k] = int(line.split()[1]) // 1024
        used = mem["MemTotal"] - mem["MemAvailable"]
        pct = used * 100 // mem["MemTotal"]
        lines.append(f"内存 {used}/{mem['MemTotal']}MB ({pct}%) · 可用 {mem['MemAvailable']}MB")
        lines.append(f"Swap {mem['SwapTotal'] - mem['SwapFree']}/{mem['SwapTotal']}MB")
    except Exception:
        pass
    try:
        df = subprocess.run(["df", "-h", "/"], capture_output=True, text=True, timeout=5).stdout.splitlines()
        f = df[1].split()
        lines.append(f"磁盘 {f[2]}/{f[1]} ({f[4]})")
    except Exception:
        pass
    services = []
    try:
        st = subprocess.run(["docker", "ps", "--filter", "name=napcat", "--format", "{{.Status}}"],
                            capture_output=True, text=True, timeout=8).stdout.strip()
        services.append(f"napcat {'●正常' if st.startswith('Up') else '●异常'}")
    except Exception:
        services.append("napcat ●未知")
    for name, unit, user in (("开播通知", "bili-live-notify", True),
                             ("弹幕机器人", "danmurobot", False),
                             ("配置面板", "danmurobot-web", False)):
        try:
            env = None
            if user:
                env = {"XDG_RUNTIME_DIR": "/run/user/0", "PATH": "/usr/local/bin:/usr/bin:/bin"}
            st = subprocess.run(["systemctl"] + (["--user"] if user else []) + ["is-active", unit],
                                capture_output=True, text=True, timeout=5,
                                env=env).stdout.strip()
            services.append(f"{name} {'●正常' if st == 'active' else '●' + st}")
        except Exception:
            services.append(f"{name} ●未知")
    lines.append("服务：" + " ".join(services))
    return "\n".join(lines)


def _persist_notify(cfg: dict, config_path: Path) -> None:
    """把内存中的 [notify] 开关/模板写回 config.toml，保留原格式与注释。"""
    import tomlkit

    doc = tomlkit.parse(config_path.read_text(encoding="utf-8"))
    notify_cfg = cfg.get("notify") or {}
    tbl = doc.setdefault("notify", tomlkit.table())
    for key in ("at_all", "include_cover"):
        if key in notify_cfg:
            tbl[key] = bool(notify_cfg[key])
    if "message_template" in notify_cfg:
        tbl["message_template"] = str(notify_cfg["message_template"])
    config_path.write_text(tomlkit.dumps(doc), encoding="utf-8")


def _two_ids(rest: str) -> tuple[int, int] | None:
    parts = rest.split()
    if len(parts) < 2 or not parts[0].isdigit() or not parts[1].isdigit():
        return None
    return int(parts[0]), int(parts[1])


def dispatch_command(cfg: dict, sub_path: Path, config_path: Path, text: str, uid: int = 0) -> str:
    """解析私聊指令并执行，返回回复文本。无法识别时返回格式说明+示例。"""
    parts = text.strip().split(None, 1)
    if not parts:
        return HELP_TEXT
    cmd = parts[0].lower()
    rest = parts[1].strip() if len(parts) > 1 else ""

    # ---- 服务器管理 ----
    if cmd in ("服务器", "server", "sys", "系统"):
        return get_server_status()

    if cmd == "重启":
        key = rest.strip().lower()
        if not key:
            return "格式：重启 弹幕 / 面板 / napcat / 通知\n重启整台服务器：重启 服务器"
        if key in ("服务器", "server", "系统"):
            PENDING_REBOOT[uid] = time.time()
            return ("⚠️ 将重启整台服务器，全部服务中断约 1 分钟。\n"
                    "60 秒内发送「确认重启」执行，否则自动取消。")
        restart_cmds = {
            "弹幕": ["systemctl", "restart", "danmurobot"],
            "面板": ["systemctl", "restart", "danmurobot-web"],
            "napcat": ["docker", "restart", "napcat"],
        }
        if key in restart_cmds:
            try:
                subprocess.run(restart_cmds[key], capture_output=True, timeout=90)
                return f"{rest.strip()} 已重启"
            except Exception as exc:
                return f"重启 {rest.strip()} 失败：{exc}"
        if key == "通知":
            subprocess.Popen(
                ["sh", "-c", "sleep 2; systemctl --user restart bili-live-notify"],
                env={"XDG_RUNTIME_DIR": "/run/user/0", "PATH": "/usr/local/bin:/usr/bin:/bin"},
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return "通知服务 2 秒后重启，重启成功会收到启动消息"
        return "可重启的对象：弹幕 / 面板 / napcat / 通知 / 服务器"

    if cmd == "确认重启":
        ts = PENDING_REBOOT.pop(uid, 0)
        if time.time() - ts > 60:
            return "没有待确认的重启请求（或已超时），请先发送「重启 服务器」"
        subprocess.Popen(
            ["sh", "-c", "sleep 2; systemctl reboot"],
            env={"PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return "收到，服务器将在 2 秒后重启，约 1 分钟后恢复"

    query_cfg = cfg.get("query") or {}
    commands = {str(c).lower() for c in (query_cfg.get("commands") or [])}
    if cmd in commands:
        return build_status_text(cfg, sub_path)
    if cmd in ("帮助", "help", "?", "？"):
        return HELP_TEXT

    if cmd in ("添加", "删除", "启用", "停用"):
        subs = load_subscriptions(sub_path)
        if cmd == "添加":
            ids = _two_ids(rest)
            if ids is None:
                return "格式：添加 房间号 群号 [备注]\n示例：添加 1726453326 668286732"
            room_id, group_id = ids
            if any(s["room_id"] == room_id and s["group_id"] == group_id for s in subs):
                return f"该订阅已存在（房间 {room_id} → 群 {group_id}）"
            if len(subs) >= 100:
                return "订阅已达上限 100 条"
            note = " ".join(rest.split()[2:])[:40]
            subs.append({"room_id": room_id, "group_id": group_id, "enabled": True, "note": note})
            save_subscriptions(sub_path, subs)
            return f"已添加：房间 {room_id} → 群 {group_id}"
        # 删除/启用/停用：支持「序号」（查询结果里的编号）或「房间号 群号」
        hit = None
        if len(rest.split()) == 1 and rest.split()[0].isdigit():
            idx = int(rest.split()[0])
            if 1 <= idx <= len(subs):
                hit = subs[idx - 1]
            else:
                return f"序号 {idx} 不存在，发送「查询」查看（当前共 {len(subs)} 条）"
        else:
            ids = _two_ids(rest)
            if ids is None:
                return f"格式：{cmd} 房间号 群号，或 {cmd} 序号\n示例：{cmd} 1726453326 668286732 或 {cmd} 2"
            room_id, group_id = ids
            hit = next((s for s in subs if s["room_id"] == room_id and s["group_id"] == group_id), None)
        if hit is None:
            return "未找到该订阅，发送「查询」查看现有订阅"
        desc = f"房间 {hit['room_id']} → 群 {hit['group_id']}"
        if cmd == "删除":
            subs.remove(hit)
            save_subscriptions(sub_path, subs)
            return f"已删除：{desc}"
        hit["enabled"] = cmd == "启用"
        save_subscriptions(sub_path, subs)
        return f"已{cmd}：{desc}"

    if cmd in ("@全体", "封面"):
        key = "at_all" if cmd == "@全体" else "include_cover"
        label = "@全体" if cmd == "@全体" else "封面图"
        val = rest.lower()
        if val in ("开", "on", "1", "true"):
            flag = True
        elif val in ("关", "off", "0", "false"):
            flag = False
        else:
            return f"格式：{cmd} 开 / {cmd} 关"
        cfg.setdefault("notify", {})[key] = flag
        try:
            _persist_notify(cfg, config_path)
        except Exception as exc:
            return f"已生效但写入配置文件失败：{exc}"
        return f"{label}已{'开启' if flag else '关闭'}（下一轮起生效）"

    if cmd == "模板":
        if not rest:
            return "格式：模板 内容\n占位符：{name}主播 {title}标题 {url}链接，\\n 表示换行\n示例：模板 {name} 开播啦！\\n{title}\\n{url}"
        template = rest.replace("\\n", "\n")
        if not any(p in template for p in ("{name}", "{title}", "{url}")):
            return "模板需至少包含一个占位符：{name} / {title} / {url}"
        cfg.setdefault("notify", {})["message_template"] = template
        try:
            _persist_notify(cfg, config_path)
        except Exception as exc:
            return f"已生效但写入配置文件失败：{exc}"
        return "模板已更新：\n" + template

    return "未识别指令。\n" + HELP_TEXT


def handle_query_event(cfg: dict, sub_path: Path, config_path: Path, body: bytes) -> None:
    """处理 OneBot v11 消息事件：白名单 QQ 私聊的查询/配置指令。"""
    try:
        event = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return
    if event.get("post_type") != "message" or event.get("message_type") != "private":
        return
    query_cfg = cfg.get("query") or {}
    allowed = {int(q) for q in (query_cfg.get("allowed_qq") or [])}
    try:
        sender = int(event.get("user_id") or 0)
    except (TypeError, ValueError):
        return
    if not sender or sender == int(event.get("self_id") or -1) or sender not in allowed:
        return

    text = str(event.get("raw_message") or "").strip()
    if not text and isinstance(event.get("message"), list):
        text = "".join(
            str(seg.get("data", {}).get("text", ""))
            for seg in event["message"]
            if isinstance(seg, dict) and seg.get("type") == "text"
        ).strip()
    if not text:
        return
    log.info("收到私聊指令：QQ %s %r", sender, text[:40])

    now = time.time()
    if now - QUERY_COOLDOWN.get(sender, 0) < QUERY_COOLDOWN_SECONDS:
        return
    QUERY_COOLDOWN[sender] = now

    reply = dispatch_command(cfg, sub_path, config_path, text, uid=sender)
    session = requests.Session()
    session.headers.update({"User-Agent": UA})
    try:
        send_private_message(session, cfg, sender, [{"type": "text", "data": {"text": reply}}])
        log.info("已回复 QQ %s（指令：%s）", sender, text.split(None, 1)[0])
    except Exception as exc:
        log.error("回复 QQ %s 失败：%s", sender, exc)


def load_state(path: Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except (ValueError, OSError) as exc:
        log.warning("状态文件读取失败，将重新初始化：%s", exc)
        return {}


def save_state(path: Path, state: dict) -> None:
    tmp = path.with_name(path.name + ".tmp")
    try:
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
    except OSError as exc:
        log.warning("状态文件写入失败：%s", exc)


PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>B站开播通知 · 管理</title>
<style>
  :root{--bg:#f5f6f8;--card:#fff;--line:#e3e6ea;--text:#1f2329;--muted:#8a919b;--accent:#00a1d6;--ok:#18a058;--warn:#d03050}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--text);font:14px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif}
  .wrap{max-width:920px;margin:36px auto;padding:0 16px}
  h1{font-size:20px;margin:0 0 4px}
  .sub{color:var(--muted);font-size:13px;margin-bottom:20px}
  .card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:18px;margin-bottom:18px}
  .bar{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px}
  .bar b{font-size:15px}
  table{width:100%;border-collapse:collapse}
  th,td{text-align:left;padding:10px 8px;border-bottom:1px solid var(--line);font-size:13px}
  th{color:var(--muted);font-weight:600}
  tr:last-child td{border-bottom:0}
  .num{font-family:ui-monospace,SFMono-Regular,Consolas,monospace}
  .tag{display:inline-block;padding:1px 8px;border-radius:20px;font-size:12px}
  .live{background:#e8f7ef;color:var(--ok)}
  .off{background:#f2f3f5;color:var(--muted)}
  .loop{background:#fff4e6;color:#d46b08}
  .err{color:var(--warn)}
  .muted{color:var(--muted)}
  input[type=text],input[type=number]{width:100%;padding:8px 10px;border:1px solid var(--line);border-radius:6px;font:inherit;outline:none}
  input:focus{border-color:var(--accent)}
  .form{display:grid;grid-template-columns:1fr 1fr 1fr auto;gap:10px;align-items:end}
  label{display:block;font-size:12px;color:var(--muted);margin-bottom:4px}
  button{padding:8px 14px;border:0;border-radius:6px;background:var(--accent);color:#fff;font:inherit;cursor:pointer}
  button.ghost{background:#fff;border:1px solid var(--line);color:var(--text)}
  button.danger{background:#fff;border:1px solid #f3c1cb;color:var(--warn)}
  button:disabled{opacity:.55;cursor:default}
  .dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--ok);margin-right:6px}
  .dot.bad{background:var(--warn)}
  #msg{position:fixed;left:50%;bottom:28px;transform:translateX(-50%);background:#1f2329;color:#fff;padding:9px 16px;border-radius:8px;font-size:13px;opacity:0;transition:.25s;pointer-events:none}
  #msg.show{opacity:1}
  .empty{color:var(--muted);text-align:center;padding:22px 0}
  @media(max-width:640px){.form{grid-template-columns:1fr 1fr}.wrap{margin:20px auto}}
</style>
</head>
<body>
<div class="wrap">
  <h1>B站开播通知</h1>
  <div class="sub" id="meta">加载中…</div>

  <div class="card">
    <div class="bar"><b>订阅列表</b><span class="muted" id="polled"></span></div>
    <table>
      <thead><tr><th>直播间</th><th>主播 / 备注</th><th>QQ群</th><th>状态</th><th>启用</th><th></th></tr></thead>
      <tbody id="rows"></tbody>
    </table>
  </div>

  <div class="card">
    <div class="bar"><b>添加订阅</b><span class="muted">保存后下一轮轮询生效，无需重启</span></div>
    <div class="form">
      <div><label>直播间房间号</label><input id="room" type="number" placeholder="如 1726453326"></div>
      <div><label>QQ 群号</label><input id="group" type="number" placeholder="如 668286732"></div>
      <div><label>备注（可选）</label><input id="note" type="text" placeholder="主播昵称"></div>
      <div><button id="add">添加</button></div>
    </div>
  </div>

  <div class="card">
    <div class="bar"><b>通知设置</b><button class="ghost" id="test">发送测试消息</button></div>
    <div class="muted" id="settings"></div>
  </div>
</div>
<div id="msg"></div>

<script>
const STATUS = {0:'未开播', 1:'直播中', 2:'轮播'};
let subs = [];
let data = null;

function toast(t){ const el=document.getElementById('msg'); el.textContent=t; el.classList.add('show'); clearTimeout(el._t); el._t=setTimeout(()=>el.classList.remove('show'),2200); }

function badge(s){
  if(s===1) return '<span class="tag live">直播中</span>';
  if(s===2) return '<span class="tag loop">轮播</span>';
  if(s===0) return '<span class="tag off">未开播</span>';
  return '<span class="tag off">—</span>';
}

function render(){
  const tb = document.getElementById('rows');
  if(!subs.length){ tb.innerHTML = '<tr><td colspan="6" class="empty">还没有订阅，先添加一个吧</td></tr>'; return; }
  tb.innerHTML = subs.map((s,i)=>{
    const room = data.status.rooms[String(s.room_id)] || {};
    const name = s.note || room.name || '—';
    const err = room.error ? ` <span class="err">(${room.error})</span>` : '';
    return `<tr>
      <td class="num">${s.room_id}</td>
      <td>${name}${err}</td>
      <td class="num">${s.group_id}</td>
      <td>${badge(room.live_status)}</td>
      <td><input type="checkbox" ${s.enabled?'checked':''} data-i="${i}" class="en"></td>
      <td><button class="danger" data-i="${i}" class="del">删除</button></td>
    </tr>`;
  }).join('');

  tb.querySelectorAll('.en').forEach(box=> box.onchange = ()=>{
    subs[+box.dataset.i].enabled = box.checked;
    save();
  });
  tb.querySelectorAll('.del').forEach(btn=> btn.onclick = ()=>{
    subs.splice(+btn.dataset.i,1);
    save();
  });

  const p = data.status;
  document.getElementById('polled').textContent = p.ts ? `上次轮询 ${new Date(p.ts*1000).toLocaleTimeString()}` : '尚未轮询';
  document.getElementById('meta').innerHTML = p.ok === false
    ? `<span class="dot bad"></span>轮询异常：${p.error||'未知错误'}`
    : `<span class="dot"></span>轮询正常 · 每 ${data.poll_interval} 秒检查一次`;
  document.getElementById('settings').textContent =
    `@全体：${data.notify.at_all?'开':'关'}　封面图：${data.notify.include_cover?'开':'关'}　模板：${data.notify.template}`;
}

async function load(){
  const r = await fetch('/api/data', {cache:'no-store'});
  if(!r.ok) return toast('加载失败：HTTP '+r.status);
  data = await r.json();
  subs = data.subscriptions;
  render();
}

async function save(){
  const r = await fetch('/api/subscriptions', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(subs)});
  const d = await r.json().catch(()=>({}));
  if(!r.ok) return toast(d.error || ('保存失败：HTTP '+r.status));
  toast('已保存');
  subs = d.subscriptions || subs;
  load();
}

document.getElementById('add').onclick = ()=>{
  const room = parseInt(document.getElementById('room').value,10);
  const group = parseInt(document.getElementById('group').value,10);
  const note = document.getElementById('note').value.trim();
  if(!room || !group) return toast('请填写合法的房间号和群号');
  if(subs.some(s=>s.room_id===room && s.group_id===group)) return toast('这条订阅已存在');
  subs.push({room_id:room, group_id:group, enabled:true, note});
  save().then(()=>{ document.getElementById('room').value=''; document.getElementById('group').value=''; document.getElementById('note').value=''; });
};

document.getElementById('test').onclick = async ()=>{
  const btn = document.getElementById('test');
  btn.disabled = true; btn.textContent = '发送中…';
  const r = await fetch('/api/test', {method:'POST'});
  const d = await r.json().catch(()=>({}));
  btn.disabled = false; btn.textContent = '发送测试消息';
  toast(d.ok ? `已发送到 ${d.sent} 个群` : ('发送失败：' + (d.error||'未知')));
};

load();
setInterval(load, 15000);
</script>
</body>
</html>
"""


def make_handler(cfg: dict, sub_path: Path, state_path: Path, config_path: Path):
    web_cfg = cfg.get("webui") or {}
    user = str(web_cfg.get("username") or "admin")
    pwd = str(web_cfg.get("password") or "")

    class Handler(BaseHTTPRequestHandler):
        server_version = "LiveNotify/1.0"

        def log_message(self, fmt, *args):  # noqa: ANN001
            pass

        def _authed(self) -> bool:
            header = self.headers.get("Authorization", "")
            if not pwd or not header.startswith("Basic "):
                return False
            try:
                raw = base64.b64decode(header[6:]).decode("utf-8")
                given_user, _, given_pwd = raw.partition(":")
            except Exception:
                return False
            return hmac.compare_digest(given_user, user) and hmac.compare_digest(given_pwd, pwd)

        def _guard(self) -> bool:
            """校验登录；对失败次数做限制，返回是否可以继续处理请求。"""
            ip = self.client_address[0]
            wait = auth_block_remaining(ip)
            if wait > 0:
                body = f"尝试次数过多，请 {max(wait // 60 + 1, 1)} 分钟后再试".encode("utf-8")
                self._send(429, body, "text/plain; charset=utf-8")
                return False
            if self._authed():
                clear_auth_failures(ip)
                return True
            record_auth_failure(ip)
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Basic realm="live-notify"')
            self.send_header("Content-Length", "0")
            self.end_headers()
            return False

        def _send(self, code: int, body: bytes, ctype: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, code: int, obj) -> None:
            self._send(code, json.dumps(obj, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")

        def do_GET(self):  # noqa: N802
            if not self._guard():
                return
            path = urlparse(self.path).path
            if path in ("/", "/index.html"):
                self._send(200, PAGE.encode("utf-8"), "text/html; charset=utf-8")
                return
            if path == "/api/data":
                with STATUS_LOCK:
                    snapshot = json.loads(json.dumps(LAST_POLL))
                notify_cfg = cfg.get("notify") or {}
                self._json(200, {
                    "subscriptions": load_subscriptions(sub_path),
                    "status": snapshot,
                    "poll_interval": int(cfg["bilibili"]["poll_interval"]),
                    "notify": {
                        "at_all": bool(notify_cfg.get("at_all")),
                        "include_cover": bool(notify_cfg.get("include_cover", True)),
                        "template": notify_cfg.get("message_template") or DEFAULT_TEMPLATE,
                    },
                })
                return
            self._json(404, {"error": "not found"})

        def do_POST(self):  # noqa: N802
            path = urlparse(self.path).path
            ip = self.client_address[0]
            log.info("收到 POST %s 来自 %s", path, ip)
            length = int(self.headers.get("Content-Length") or 0)
            if length > 64 * 1024:
                self._json(413, {"error": "请求体过大"})
                return
            body = self.rfile.read(length) if length else b"{}"

            # OneBot 事件上报：不走 Basic Auth，安全边界是 QQ 白名单 + 指令完全匹配
            # 始终返回 200 空 JSON，避免 NapCat 认为上报失败而重试刷屏
            if path == EVENT_PATH:
                try:
                    handle_query_event(cfg, sub_path, config_path, body)
                except Exception as exc:
                    log.warning("处理事件上报异常：%s", exc)
                self._send(200, b"{}", "application/json; charset=utf-8")
                return

            if not self._guard():
                return

            if path == "/api/subscriptions":
                try:
                    subs = normalize_subscriptions(json.loads(body or b"[]"))
                    save_subscriptions(sub_path, subs)
                except ValueError as exc:
                    self._json(400, {"error": str(exc)})
                    return
                except (json.JSONDecodeError, OSError) as exc:
                    self._json(400, {"error": f"保存失败：{exc}"})
                    return
                log.info("网页更新订阅：%d 条", len(subs))
                self._json(200, {"ok": True, "subscriptions": subs})
                return

            if path == "/api/test":
                subs = load_subscriptions(sub_path)
                groups = sorted({s["group_id"] for s in subs})
                if not groups:
                    self._json(400, {"error": "还没有订阅，无法确定要发到哪个群"})
                    return
                session = requests.Session()
                session.headers.update({"User-Agent": UA})
                sent, errors = 0, []
                for gid in groups:
                    try:
                        if _use_qq_official(cfg):
                            send_group_message(session, cfg, gid, text_only="【测试】B站开播通知已连接成功")
                        else:
                            send_group_message(session, cfg, gid,
                                               segments=[{"type": "text", "data": {"text": "【测试】B站开播通知已连接成功"}}])
                        sent += 1
                    except Exception as exc:
                        errors.append(f"{gid}: {exc}")
                if errors:
                    self._json(500, {"ok": False, "error": "；".join(errors)})
                else:
                    self._json(200, {"ok": True, "sent": sent})
                return

            self._json(404, {"error": "not found"})

    return Handler


def _startup_notify(cfg: dict) -> None:
    """启动后向管理员发送启动消息。官方机器人路径需要 user_openid，暂跳过。"""
    if _use_qq_official(cfg):
        # 官方机器人私聊需要 user_openid（非数字 QQ 号），暂不发启动通知
        log.info("QQ 官方机器人已启动（启动通知暂未发送：需要 user_openid 映射）")
        return
    qqs = (cfg.get("query") or {}).get("allowed_qq") or []
    if not qqs:
        return
    if not WS_CONNECTED.wait(timeout=45):
        log.warning("启动通知：等待 NapCat 连接超时，跳过")
        return
    time.sleep(2)
    try:
        session = requests.Session()
        session.headers.update({"User-Agent": UA})
        send_private_message(session, cfg, int(qqs[0]), text_only="🤖 通知机器人已启动\n" + get_server_status())
        log.info("已向管理员 QQ %s 发送启动消息", qqs[0])
    except Exception as exc:
        log.warning("启动通知发送失败：%s", exc)


def start_ws_listener(cfg: dict, sub_path: Path, config_path: Path) -> None:
    """后台线程：连接 NapCat 容器内 WS 接收消息事件（HTTP 上报在本版 NapCat 发空 body，不可用）。

    WS 端口只在容器网络内监听，宿主机通过 docker 网桥直连固定 IP，不占用宿主机端口。
    断线自动重连；未配置白名单则不启动。
    """
    query_cfg = cfg.get("query") or {}
    if not (query_cfg.get("allowed_qq") or []):
        return
    import asyncio

    import websockets

    url = str(query_cfg.get("ws_url") or "ws://172.19.0.10:3001")

    async def _listen() -> None:
        while True:
            try:
                # NapCat 用心跳事件保活，不走 WS 协议层 ping
                async with websockets.connect(url, ping_interval=None) as ws:
                    WS_CONNECTED.set()
                    log.info("已连接 NapCat 事件通道（%s）", url)
                    try:
                        async for raw in ws:
                            try:
                                handle_query_event(cfg, sub_path, config_path, raw.encode("utf-8", "replace"))
                            except Exception as exc:
                                log.warning("处理事件异常：%s", exc)
                    finally:
                        WS_CONNECTED.clear()
            except Exception as exc:
                log.warning("NapCat 事件通道断开（%s），5 秒后重连", exc)
                await asyncio.sleep(5)

    threading.Thread(target=lambda: asyncio.run(_listen()), name="ws-listener", daemon=True).start()


def start_web(cfg: dict, sub_path: Path, state_path: Path, config_path: Path) -> None:
    web_cfg = cfg.get("webui") or {}
    if not web_cfg.get("password"):
        log.warning("未配置 webui.password，管理页面已禁用")
        return
    host = str(web_cfg.get("host") or "0.0.0.0")
    port = int(web_cfg.get("port") or 9900)
    try:
        httpd = ThreadingHTTPServer((host, port), make_handler(cfg, sub_path, state_path, config_path))
    except OSError as exc:
        log.error("管理页面启动失败（%s:%s）：%s", host, port, exc)
        return
    httpd.daemon_threads = True
    threading.Thread(target=httpd.serve_forever, name="webui", daemon=True).start()
    log.info("管理页面已启动：http://%s:%s", host, port)


def poll_cycle(
    session: requests.Session,
    cfg: dict,
    subs: list[dict],
    state: dict,
    state_path: Path,
) -> bool:
    """执行一轮检查。返回本轮是否成功。"""
    notify_cfg = cfg.get("notify") or {}
    rooms: dict[int, dict] = {}
    changed = False

    for sub in subs:
        if not sub["enabled"]:
            continue
        room_id, group_id = sub["room_id"], sub["group_id"]
        if room_id not in rooms:
            try:
                info = bili_get(session, ROOM_INFO_API, {"room_id": room_id})
                rooms[room_id] = {"info": info, "name": fetch_anchor_name(session, room_id), "error": ""}
            except Exception as exc:
                rooms[room_id] = {"info": None, "name": "", "error": str(exc)}
        item = rooms[room_id]
        if item["info"] is None:
            continue

        info = item["info"]
        name = item["name"]
        status = int(info.get("live_status") or 0)
        title = (info.get("title") or "").strip()

        key = f"{room_id}:{group_id}"
        record = state.get(key) or {}
        prev = record.get("live_status")

        if prev is None:
            if status == 1 and notify_cfg.get("notify_on_startup"):
                if not notify(session, cfg, info, name, room_id, group_id):
                    continue
            log.info("初始化状态：房间 %s 群 %s → %s", room_id, group_id, STATUS_NAMES.get(status, status))
            state[key] = {"live_status": status, "title": title}
            changed = True
        elif prev != 1 and status == 1:
            log.info("检测到开播：%s（房间 %s）", title, room_id)
            if notify(session, cfg, info, name, room_id, group_id):
                state[key] = {"live_status": status, "title": title, "live_time": info.get("live_time") or ""}
                changed = True
        elif prev == 1 and status != 1:
            log.info("检测到下播：%s（房间 %s）", STATUS_NAMES.get(status, status), room_id)
            state[key] = {"live_status": status, "title": title}
            changed = True
        elif record.get("title") != title:
            state[key] = {**record, "live_status": status, "title": title}
            changed = True

    if changed:
        save_state(state_path, state)

    errors = {str(rid): item["error"] for rid, item in rooms.items() if item["error"]}
    snapshot = {
        "ts": int(time.time()),
        "ok": not errors,
        "error": "；".join(f"{rid}: {e}" for rid, e in errors.items()),
        "rooms": {
            str(rid): {
                "live_status": int(item["info"].get("live_status") or 0) if item["info"] else None,
                "title": (item["info"].get("title") or "") if item["info"] else "",
                "name": item["name"],
                "error": item["error"],
            }
            for rid, item in rooms.items()
        },
    }
    with STATUS_LOCK:
        LAST_POLL.update(snapshot)
    return not errors


def run() -> int:
    parser = argparse.ArgumentParser(description="B站开播 → QQ群通知")
    parser.add_argument("-c", "--config", default=str(BASE_DIR / "config.toml"), help="配置文件路径")
    parser.add_argument("--test", action="store_true", help="向所有订阅群发送测试消息后退出")
    parser.add_argument("--once", action="store_true", help="只跑一轮轮询就退出（用于 GitHub Actions / cron）")
    parser.add_argument("--no-web", action="store_true", help="不启动管理页面")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    cfg = load_config(Path(args.config))

    storage = cfg.get("storage") or {}
    state_path = BASE_DIR / str(storage.get("state_file") or "state.json")
    sub_path = BASE_DIR / str(storage.get("subscriptions_file") or "subscriptions.json")
    group_map_path = BASE_DIR / "group_map.json"

    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Referer": "https://live.bilibili.com/"})

    # ---- 初始化 QQ 官方机器人客户端 ----
    global QQ_CLIENT
    if _use_qq_official(cfg):
        qqoff_cfg = cfg["qq_official"]
        QQ_CLIENT = QQOfficialClient(
            appid=qqoff_cfg["appid"],
            secret=qqoff_cfg["secret"],
            group_map_path=group_map_path,
        )
        # 预先触发一次 token 获取，提前暴露配置错误
        try:
            QQ_CLIENT.get_token()
            log.info("QQ 官方机器人 token 获取成功")
        except Exception as exc:
            log.error("QQ 官方机器人 token 获取失败：%s", exc)
            return 1
        # 启动 WebSocket 网关监听
        QQ_CLIENT.start()
        log.info("QQ 官方机器人网关监听已启动")
        time.sleep(1)  # 等网关连上
        missing = [int(s["group_id"]) for s in load_subscriptions(sub_path)
                   if s.get("enabled") and not QQ_CLIENT.get_group_openid(int(s["group_id"]))]
        if missing:
            log.warning("以下群号未绑定 group_openid，通知会发送失败：%s", missing)
            log.warning("请在对应群里 @机器人 发送指令，然后在私聊用「绑定群 <openid> <群号>」关联")

    subs = load_subscriptions(sub_path)

    if args.test:
        groups = sorted({s["group_id"] for s in subs})
        if not groups:
            log.error("没有订阅，无法确定测试消息发往哪个群")
            return 1
        failed = 0
        for gid in groups:
            try:
                if _use_qq_official(cfg):
                    send_group_message(session, cfg, gid, text_only="【测试】B站开播通知已连接成功")
                else:
                    send_group_message(session, cfg, gid,
                                       segments=[{"type": "text", "data": {"text": "【测试】B站开播通知已连接成功"}}])
                log.info("测试消息发送成功，群号 %s", gid)
            except Exception as exc:
                failed += 1
                log.error("测试消息发送失败（群 %s）：%s", gid, exc)
        return 1 if failed else 0

    if args.once:
        # ---- GitHub Actions / cron 模式：跑一轮就退出 ----
        state = load_state(state_path)
        poll_cycle(session, cfg, subs, state, state_path)
        return 0

    interval = int(cfg["bilibili"]["poll_interval"])
    config_path = Path(args.config)
    if not args.no_web:
        start_web(cfg, sub_path, state_path, config_path)
    # NapCat WS 监听器仅在未启用官方机器人时启动
    if not _use_qq_official(cfg):
        start_ws_listener(cfg, sub_path, config_path)
    threading.Thread(target=_startup_notify, args=(cfg,), name="startup-notify", daemon=True).start()

    state = load_state(state_path)
    log.info("开始监听 %d 条订阅，每 %d 秒检查一次", sum(1 for s in subs if s["enabled"]), interval)

    stopping = False

    def _stop(signum, frame):  # noqa: ANN001
        nonlocal stopping
        stopping = True
        log.info("收到退出信号，正在停止…")

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    failures = 0
    while not stopping:
        subs = load_subscriptions(sub_path)  # 每轮重新读取，网页改动即时生效
        try:
            ok = poll_cycle(session, cfg, subs, state, state_path)
            if ok and failures:
                log.info("B站接口恢复正常")
            failures = 0 if ok else failures + 1
        except Exception as exc:
            failures += 1
            log.warning("轮询异常（连续 %d 次）：%s", failures, exc)
            with STATUS_LOCK:
                LAST_POLL.update({"ts": int(time.time()), "ok": False, "error": str(exc)})

        for _ in range(interval):
            if stopping:
                break
            time.sleep(1)

    log.info("已停止监听")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
