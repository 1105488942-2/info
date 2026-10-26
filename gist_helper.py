import json
import urllib.request
import urllib.error
import os
import sys

def download_state():
    gist_id = os.environ.get('GIST_ID', '')
    token = os.environ.get('GITHUB_TOKEN', '')
    if not gist_id:
        print('ERROR: GIST_ID not set')
        open('state.json', 'w').write('{}')
        return
    headers = {'Accept': 'application/vnd.github+json'}
    if token:
        headers['Authorization'] = f'Bearer {token}'
    req = urllib.request.Request(
        f'https://api.github.com/gists/{gist_id}',
        headers=headers
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            d = json.load(resp)
        content = d.get('files', {}).get('state.json', {}).get('content', '')
        print(f'Gist download OK, content length: {len(content)}')
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:500] if hasattr(e, 'read') else ''
        print(f'Gist HTTP error {e.code}: {body}')
        content = ''
    except Exception as e:
        print(f'Gist download error: {type(e).__name__}: {e}')
        content = ''
    open('state.json', 'w').write(content or '{}')

def upload_state():
    gist_id = os.environ.get('GIST_ID', '')
    token = os.environ.get('GITHUB_TOKEN', '')
    if not gist_id or not token:
        print(f'Skip upload: gist_id={bool(gist_id)}, token={bool(token)}')
        return
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
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f'Gist upload OK ({resp.status})')
    except urllib.error.HTTPError as e:
        body2 = e.read().decode()[:500] if hasattr(e, 'read') else ''
        print(f'Gist upload HTTP error {e.code}: {body2}')
    except Exception as e:
        print(f'Gist upload error: {type(e).__name__}: {e}')

if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'download'
    if cmd == 'upload':
        upload_state()
    else:
        download_state()
