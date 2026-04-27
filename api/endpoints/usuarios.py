from typing import Any, Dict, Optional, Tuple

import requests

from api.client import ensure_trailing_slash


def call_create_user_api(base_url: str, payload: Dict[str, Any], timeout_sec: int = 60) -> Dict[str, Any]:
    url = ensure_trailing_slash(base_url) + "api/v1/user/"
    try:
        resp = requests.post(url, json=payload, timeout=timeout_sec)
        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text}
        return {"http_status": resp.status_code, "data": data}
    except Exception as e:
        return {"http_status": None, "data": {"success": False, "message": f"Error de conexion: {e}"}}


def call_reset_api_key(base_url: str, payload: Dict[str, Any], timeout_sec: int = 60) -> Dict[str, Any]:
    url = ensure_trailing_slash(base_url) + "api/v1/user/reset-key/"
    email_param = None
    if isinstance(payload, dict):
        email_param = payload.get("email") or payload.get("mail")
    params: Optional[Dict[str, str]] = {"email": email_param} if email_param else None
    try:
        resp = requests.post(url, params=params, timeout=timeout_sec)
        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text}
        return {"http_status": resp.status_code, "data": data}
    except Exception as e:
        return {"http_status": None, "data": {"success": False, "message": f"Error de conexion: {e}"}}


def call_consultas_disponibles(base_url: str, email_path: str, headers: Dict[str, str],
                               timeout_sec: int = 60) -> Tuple[Optional[int], Optional[Dict[str, Any]], Optional[int], Optional[str]]:
    url = ensure_trailing_slash(base_url) + f"api/v1/user/consultas/{email_path}"
    try:
        resp = requests.get(url, headers=headers, timeout=timeout_sec)
        js = None
        try:
            js = resp.json()
        except Exception:
            return resp.status_code, None, None, f"Respuesta no-JSON: {resp.text[:500]}"
        cd = js.get("consultas_disponibles") if isinstance(js, dict) else None
        return resp.status_code, js, cd, None
    except Exception as e:
        return None, None, None, f"Error de conexion: {e}"
