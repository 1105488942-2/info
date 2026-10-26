import json
import urllib.request
import os
import sys

def download_state():
    gist_id = os.environ['GIST_ID']
    token = os.environ['GITHUB_TOKEN']
    req = urllib.request.Request(
        f'https://api.github.com/gists/{gist_id}',
        headers={
            'Authorization': f'Bearer {token}',
            'Accept': 'application/vnd.github+json',
        }
    )
    try:
        with urllib.request.urlopen(req) as resp:
            d = json.load(resp)
        content = d.get('files', {}).get('state.json', {}).get('content', '')
    except Exception as e:
        print(f'下载 state.json 失败: {e}')
        content = ''
    open('state.json', 'w').write(content or '{}')
    print(f'state.json 已准备 ({len(content)} bytes)')

def upload_state():
    gist_id = os.environ['GIST_ID']
    token = os.environ['GITHUB_TOKEN']
    content = open('state.json').read()
    body = json.dumps({'files': {'state.json': {'content': content}}}).encode()
    req = urllib.request.Request(
        f'https://api.github.com/gists/{gist_id}',
        data=body, method='PATCH',
        headers={
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
            'Accept': 'application/vnd.github+json',
        }
    )
    with urllib.request.urlopen(req) as resp:
        print(f'state.json 已保存到 Gist ({resp.status})')

if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'download'
    if cmd == 'upload':
        upload_state()
    else:
        download_state()
