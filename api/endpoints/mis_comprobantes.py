from typing import Any, Dict

from api.client import ensure_trailing_slash, safe_post


def call_consulta(base_url: str, headers: Dict[str, str], payload: Dict[str, Any],
                  timeout_sec: int = 120, max_retries: int = 2) -> Dict[str, Any]:
    url = ensure_trailing_slash(base_url) + "api/v1/mis_comprobantes/consulta"
    return safe_post(url, headers, payload, timeout_sec=timeout_sec, max_retries=max_retries)
