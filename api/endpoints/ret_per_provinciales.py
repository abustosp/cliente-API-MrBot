from typing import Any, Dict

from api.client import ensure_trailing_slash, safe_post


def call_arba_consulta(base_url: str, headers: Dict[str, str], payload: Dict[str, Any],
                       timeout_sec: int = 120) -> Dict[str, Any]:
    url = ensure_trailing_slash(base_url) + "api/v1/retenciones_percepciones_iibb/arba/consulta"
    return safe_post(url, headers, payload, timeout_sec=timeout_sec)


def call_agip_consulta(base_url: str, headers: Dict[str, str], payload: Dict[str, Any],
                       timeout_sec: int = 120) -> Dict[str, Any]:
    url = ensure_trailing_slash(base_url) + "api/v1/retenciones_percepciones_iibb/agip/consulta"
    return safe_post(url, headers, payload, timeout_sec=timeout_sec)


def call_misiones_consulta(base_url: str, headers: Dict[str, str], payload: Dict[str, Any],
                           timeout_sec: int = 120) -> Dict[str, Any]:
    url = ensure_trailing_slash(base_url) + "api/v1/retenciones_percepciones_iibb/misiones/consulta"
    return safe_post(url, headers, payload, timeout_sec=timeout_sec)
