import os
import re
import uuid
from urllib.parse import urlparse, unquote

import requests


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = os.path.basename(parsed.path)
    # 随机字符串
    name = name or str(uuid.uuid4()) + ".mp4"
    root, ext = os.path.splitext(name)
    ext = ext or ".mp4"
    root = re.sub(r"[\0-\x1f]", "", root)
    root = root.strip()
    root = root.replace("/", "_").replace("\\", "_")
    root_bytes = root.encode("utf-8")
    if len(root_bytes) > 200:
        root = root_bytes[:200].decode("utf-8", errors="ignore")
    safe = root or "file"
    return f"{safe}{ext}"


def download_to_file(url: str, dest_dir: str):
    ensure_dir(dest_dir)
    fname = filename_from_url(url)
    file_path = os.path.join(dest_dir, fname)
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(file_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    size = os.path.getsize(file_path)
    return file_path, fname, size
