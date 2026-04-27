from typing import Any, Dict

from api.client import ensure_trailing_slash, safe_post


def call_sifere_consulta(base_url: str, headers: Dict[str, str], payload: Dict[str, Any],
                         timeout_sec: int = 600) -> Dict[str, Any]:
    url = ensure_trailing_slash(base_url) + "api/v1/sifere/consulta"
    return safe_post(url, headers, payload, timeout_sec=timeout_sec)
