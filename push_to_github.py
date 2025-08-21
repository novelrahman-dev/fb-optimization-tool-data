#!/usr/bin/env python3
"""
Push a local file to GitHub.
Two modes:
  - CONTENTS API (default): commits the file at a path on a branch (<= 100MB limit)
  - RELEASES API: uploads file as a release asset (<= 2GB per asset)

Env vars:
  OUTPUT_PATH              Local file path to upload (default: data/export.csv.gz)
  GITHUB_TOKEN             Personal Access Token with 'repo' scope (required)
  GITHUB_REPO              'owner/repo' (required)
  GITHUB_BRANCH            Branch to commit to (default: main)  [contents mode]
  GITHUB_DEST_PATH         Path in repo to write (default: data/export.csv.gz) [contents mode]
  GITHUB_COMMIT_MESSAGE    Commit message (optional)            [contents mode]
  GITHUB_UPLOAD_MODE       'contents' (default) or 'release'
  GITHUB_RELEASE_TAG       Tag for the release (default: data-YYYYMMDD)
  GITHUB_RELEASE_NAME      Optional human-readable release name
  GITHUB_ASSET_NAME        Asset filename (default: basename(OUTPUT_PATH)) [release mode]
  GITHUB_API_URL           Override base API URL (default: https://api.github.com)
"""

import os, sys, base64, json, datetime
from pathlib import Path

try:
    import requests
except Exception:
    print("Missing dependency: requests. Run: pip install requests", file=sys.stderr)
    raise

def env(name, default=None, required=False):
    v = os.getenv(name, default)
    if required and (v is None or str(v).strip() == ""):
        print(f"Missing required env var: {name}", file=sys.stderr); sys.exit(1)
    return v

def github_api(path, token, method="GET", base_url=None, **kwargs):
    base = base_url or "https://api.github.com"
    url = f"{base.rstrip('/')}/{path.lstrip('/')}"
    headers = kwargs.pop("headers", {})
    headers.setdefault("Authorization", f"Bearer {token}")
    headers.setdefault("Accept", "application/vnd.github+json")
    r = requests.request(method, url, headers=headers, **kwargs)
    return r

def push_via_contents_api(token, repo, branch, dest_path, local_path, message, api_url):
    # Check file size limit
    size = Path(local_path).stat().st_size
    if size > 100 * 1024 * 1024:
        print("❌ File is larger than 100MB; CONTENTS API will fail. Use GITHUB_UPLOAD_MODE=release.", file=sys.stderr)
        sys.exit(2)

    with open(local_path, "rb") as f:
        content_b64 = base64.b64encode(f.read()).decode("utf-8")

    # Get existing SHA if file exists (to update)
    get = github_api(f"repos/{repo}/contents/{dest_path}?ref={branch}", token, base_url=api_url)
    sha = get.json().get("sha") if get.status_code == 200 else None

    payload = {
        "message": message or f"data: update {dest_path}",
        "content": content_b64,
        "branch": branch,
        "committer": {"name": "automation", "email": "actions@users.noreply.github.com"}
    }
    if sha:
        payload["sha"] = sha

    put = github_api(f"repos/{repo}/contents/{dest_path}", token, method="PUT", json=payload, base_url=api_url)
    if put.status_code not in (200, 201):
        print("❌ Upload failed:", put.status_code, put.text, file=sys.stderr)
        sys.exit(3)
    print(f"✅ Committed {local_path} to {repo}@{branch}:{dest_path}")

def create_or_get_release(token, repo, tag, name, api_url):
    # Try get release by tag
    get = github_api(f"repos/{repo}/releases/tags/{tag}", token, base_url=api_url)
    if get.status_code == 200:
        return get.json()
    # Create
    payload = {"tag_name": tag}
    if name:
        payload["name"] = name
    create = github_api(f"repos/{repo}/releases", token, method="POST", json=payload, base_url=api_url)
    if create.status_code not in (201,):
        print("❌ Create release failed:", create.status_code, create.text, file=sys.stderr)
        sys.exit(4)
    return create.json()

def upload_asset_to_release(token, release, local_path, asset_name, api_url):
    upload_url = release["upload_url"].split("{", 1)[0]
    params = {"name": asset_name}
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream",
        "Accept": "application/vnd.github+json",
    }
    with open(local_path, "rb") as f:
        data = f.read()
    # If asset with same name exists, delete it first
    assets = github_api(release["assets_url"].replace("https://api.github.com/", ""), token, base_url=api_url)
    if assets.status_code == 200:
        for a in assets.json():
            if a.get("name") == asset_name:
                delr = github_api(f"repos/{release['repository']['full_name']}/releases/assets/{a['id']}", token, method="DELETE", base_url=api_url)
                break
    r = requests.post(upload_url, params=params, data=data, headers=headers)
    if r.status_code not in (201,):
        print("❌ Asset upload failed:", r.status_code, r.text, file=sys.stderr)
        sys.exit(5)
    print(f"✅ Uploaded asset {asset_name} to release {release.get('tag_name')}")

def main():
    token   = env("GITHUB_TOKEN", required=True)
    repo    = env("GITHUB_REPO", required=True)
    mode    = env("GITHUB_UPLOAD_MODE", default="contents").lower()
    api_url = env("GITHUB_API_URL", default="https://api.github.com")

    local   = env("OUTPUT_PATH", default="data/export.csv.gz")

    if mode == "contents":
        branch   = env("GITHUB_BRANCH", default="main")
        dest     = env("GITHUB_DEST_PATH", default="data/export.csv.gz")
        message  = env("GITHUB_COMMIT_MESSAGE", default=None)
        push_via_contents_api(token, repo, branch, dest, local, message, api_url)
    elif mode == "release":
        today = datetime.datetime.utcnow().strftime("%Y%m%d")
        tag   = env("GITHUB_RELEASE_TAG", default=f"data-{today}")
        name  = env("GITHUB_RELEASE_NAME", default=f"Data export {today}")
        asset = env("GITHUB_ASSET_NAME", default=os.path.basename(local))
        rel = create_or_get_release(token, repo, tag, name, api_url)
        upload_asset_to_release(token, rel, local, asset, api_url)
    else:
        print("GITHUB_UPLOAD_MODE must be 'contents' or 'release'", file=sys.stderr)
        sys.exit(6)

if __name__ == "__main__":
    main()
