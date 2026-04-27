from typing import Any, Dict, Optional

import requests


def ensure_trailing_slash(url: str) -> str:
    return url if url.endswith("/") else url + "/"


def build_headers(x_api_key: Optional[str], email: Optional[str]) -> Dict[str, str]:
    headers: Dict[str, str] = {"Content-Type": "application/json"}
    if x_api_key:
        headers["x-api-key"] = x_api_key
    if email:
        headers["email"] = email
    return headers


def safe_post(
    url: str,
    headers: Dict[str, str],
    payload: Dict[str, Any],
    timeout_sec: int = 120,
    max_retries: int = 0,
) -> Dict[str, Any]:
    last_exc: Optional[Exception] = None
    for _ in range(max_retries + 1):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=timeout_sec)
            try:
                data = resp.json()
            except Exception:
                data = {"raw_text": resp.text}
            return {"http_status": resp.status_code, "data": data}
        except Exception as e:
            last_exc = e
    return {"http_status": None, "data": {"success": False, "message": f"Error de conexion: {last_exc}"}}


def safe_get(
    url: str,
    headers: Dict[str, str],
    timeout_sec: int = 60,
) -> Dict[str, Any]:
    try:
        resp = requests.get(url, headers=headers, timeout=timeout_sec)
        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text}
        return {"http_status": resp.status_code, "data": data}
    except Exception as e:
        return {"http_status": None, "data": {"success": False, "message": f"Error de conexion: {e}"}}
