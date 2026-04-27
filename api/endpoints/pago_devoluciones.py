from typing import Any, Dict

from api.client import ensure_trailing_slash, safe_post


def call_pago_devoluciones_consulta(base_url: str, headers: Dict[str, str], payload: Dict[str, Any],
                                    timeout_sec: int = 120) -> Dict[str, Any]:
    url = ensure_trailing_slash(base_url) + "api/v1/pago_devoluciones/consulta"
    return safe_post(url, headers, payload, timeout_sec=timeout_sec)
