import os
import re
import zipfile
from typing import Optional
from urllib.parse import urlparse, unquote

import requests


def sanitize_filename(name: str) -> str:
    name = unquote(name)
    name = re.sub(r"[\\/*?\"<>|:#]", "_", name)
    name = name.strip().strip(".")
    return name or "archivo"


def infer_filename_from_url(url: str) -> str:
    try:
        path = urlparse(url).path
        base = os.path.basename(path)
        if base:
            return sanitize_filename(base)
    except Exception:
        pass
    return "archivo"


def get_filename_from_headers(resp: requests.Response) -> Optional[str]:
    cd = resp.headers.get("Content-Disposition")
    if not cd:
        return None
    m = re.search(r'filename\*=UTF-8\'\'(.+)', cd)
    if m:
        return sanitize_filename(unquote(m.group(1)))
    m = re.search(r'filename="?([^"]+)"?', cd)
    if m:
        return sanitize_filename(m.group(1))
    return None


def is_zip_bytes(b: bytes, content_type: Optional[str], fallback_name: Optional[str]) -> bool:
    if content_type and "zip" in content_type.lower():
        return True
    if fallback_name and fallback_name.lower().endswith(".zip"):
        return True
    return b.startswith(b"PK\x03\x04")


def write_unique(zf: zipfile.ZipFile, arcname: str, data: bytes) -> str:
    base_dir, fname = os.path.split(arcname)
    base, ext = os.path.splitext(fname)
    candidate = arcname
    k = 1
    while candidate in zf.namelist():
        candidate = os.path.join(base_dir, f"{base}_{k}{ext}")
        k += 1
    zf.writestr(candidate, data)
    return candidate
