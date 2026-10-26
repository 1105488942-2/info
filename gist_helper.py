import json
import os
import sys
import urllib.error
import urllib.request

API = 'https://api.github.com'
FILES = ('state.json', 'group_map.json')
HINT = '（401/404 通常是 GIST_TOKEN 缺少 gist 权限或 GIST_ID 不对）'


def _gist_id():
    return os.environ.get('GIST_ID', '').strip()


def _token():
    # GITHUB_TOKEN 是仓库级 token，没有 gist 权限，所以优先用个人 PAT
    return (os.environ.get('GIST_TOKEN') or os.environ.get('GITHUB_TOKEN') or '').strip()


def _headers(token):
    headers = {'Accept': 'application/vnd.github+json'}
    if token:
        headers['Authorization'] = f'Bearer {token}'
    return headers


def _write(path, content):
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content or '{}')


def download_state():
    gist_id = _gist_id()
    if not gist_id:
        print('ERROR: GIST_ID not set，本轮 state 从空开始')
        _write('state.json', '{}')
        return
    req = urllib.request.Request(f'{API}/gists/{gist_id}', headers=_headers(_token()))
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            gist = json.load(resp)
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:500] if hasattr(e, 'read') else ''
        print(f'Gist download HTTP error {e.code}: {body} {HINT}')
        _write('state.json', '{}')
        return
    except Exception as e:
        print(f'Gist download error: {type(e).__name__}: {e}')
        _write('state.json', '{}')
        return

    files = gist.get('files') or {}
    for name in FILES:
        if name not in files:
            continue
        content = (files[name] or {}).get('content') or ''
        _write(name, content)
        print(f'{name} 已从 Gist 下载（{len(content)} 字节）')
    if 'state.json' not in files:
        _write('state.json', '{}')
        print('Gist 中没有 state.json，按空状态继续')


def upload_state():
    gist_id = _gist_id()
    token = _token()
    if not gist_id or not token:
        print(f'Skip upload: gist_id={bool(gist_id)}, token={bool(token)}')
        return
    payload = {}
    for name in FILES:
        if not os.path.exists(name):
            continue
        with open(name, encoding='utf-8') as f:
            payload[name] = {'content': f.read()}
    if not payload:
        print('Skip upload: 本地没有可上传的文件')
        return
    body = json.dumps({'files': payload}).encode()
    req = urllib.request.Request(
        f'{API}/gists/{gist_id}',
        data=body, method='PATCH',
        headers={
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
            'Accept': 'application/vnd.github+json',
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f'Gist upload OK ({resp.status}): {", ".join(payload)}')
    except urllib.error.HTTPError as e:
        body2 = e.read().decode()[:500] if hasattr(e, 'read') else ''
        print(f'Gist upload HTTP error {e.code}: {body2} {HINT}')
    except Exception as e:
        print(f'Gist upload error: {type(e).__name__}: {e}')


if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'download'
    if cmd == 'upload':
        upload_state()
    else:
        download_state()
