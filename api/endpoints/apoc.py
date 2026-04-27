from typing import Any, Dict

from api.client import ensure_trailing_slash, safe_get


def call_apoc_consulta(base_url: str, headers: Dict[str, str], cuit: str,
                       timeout_sec: int = 60) -> Dict[str, Any]:
    url = ensure_trailing_slash(base_url) + f"api/v1/apoc/consulta/{cuit}"
    return safe_get(url, headers, timeout_sec=timeout_sec)
