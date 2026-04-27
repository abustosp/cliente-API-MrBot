import re
from typing import Any, Dict, Optional


URL_REGEX = re.compile(r"https?://[^\s\"'<>]+")
CUIT_REGEX = re.compile(r"(?<!\d)(\d{11})(?!\d)")


def pick_url_fields(resp_data: Dict[str, Any]) -> Dict[str, Optional[str]]:
    out = {
        "mis_comprobantes_emitidos_url_s3": None,
        "mis_comprobantes_emitidos_url_minio": None,
        "mis_comprobantes_recibidos_url_s3": None,
        "mis_comprobantes_recibidos_url_minio": None,
    }
    for k in out.keys():
        if isinstance(resp_data, dict) and k in resp_data and resp_data[k]:
            out[k] = resp_data[k]
    return out
