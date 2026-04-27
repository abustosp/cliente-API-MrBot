from typing import Any, Dict, List, Optional
import re
import json
from datetime import datetime
from io import BytesIO

import pandas as pd

from api.client import ensure_trailing_slash, safe_post


def call_srt_alicuotas_consulta(base_url: str, headers: Dict[str, str], payload: Dict[str, Any],
                                timeout_sec: int = 120) -> Dict[str, Any]:
    url = ensure_trailing_slash(base_url) + "api/v1/srt/alicuotas/consulta"
    return safe_post(url, headers, payload, timeout_sec=timeout_sec)


def _to_float(value: str) -> Optional[float]:
    text = str(value or "").strip()
    if not text:
        return None
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _parse_alicuota_text(alicuota_text: str) -> tuple:
    text = str(alicuota_text or "")
    var_match = re.search(r"variable\s*:\s*([0-9.,-]+)%", text, flags=re.IGNORECASE)
    fija_match = re.search(r"suma\s*fija\s*:\s*\$\s*([0-9.,-]+)", text, flags=re.IGNORECASE)
    suma_variable = _to_float(var_match.group(1)) if var_match else None
    suma_fija = _to_float(fija_match.group(1)) if fija_match else None
    return suma_fija, suma_variable


def _parse_ciiu_text(ciiu_text: str) -> tuple:
    text = str(ciiu_text or "").strip()
    if not text:
        return "", ""
    match = re.match(r"^(\d+)\s*-\s*(.+)$", text)
    if not match:
        return "", text
    return match.group(1).strip(), match.group(2).strip()


def _extract_ok_block_values(block: Any) -> tuple:
    if not isinstance(block, dict):
        return "", "", None, None
    rows = block.get("rows")
    if not isinstance(rows, list):
        return "", "", None, None
    ciiu_text = ""
    alicuota_text = ""
    for row in rows:
        if not isinstance(row, list) or len(row) < 2:
            continue
        key = str(row[0]).strip().lower()
        value = str(row[1]).strip()
        if "ciiu" in key:
            ciiu_text = value
        if "alicuota" in key or "alícuota" in key:
            alicuota_text = value
    ciiu_num, ciiu_desc = _parse_ciiu_text(ciiu_text)
    suma_fija, suma_variable = _parse_alicuota_text(alicuota_text)
    return ciiu_num, ciiu_desc, suma_fija, suma_variable


def normalize_srt_consulta_rows(consultas: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not isinstance(consultas, list):
        return rows
    for consulta in consultas:
        if not isinstance(consulta, dict):
            rows.append({
                "CUIT": "",
                "afiliacion": "error de formato en consulta",
                "CIUU (numero)": "",
                "Descripcion CIIU": "",
                "suma fija": None,
                "suma variable": None,
            })
            continue
        cuit = str(consulta.get("cuit", "") or "").strip()
        status = str(consulta.get("status", "") or "").strip().upper()
        data = consulta.get("data")
        if status == "SIN_AFILIACION_VIGENTE":
            rows.append({
                "CUIT": cuit,
                "afiliacion": "consultado no tiene afiliacion vigente",
                "CIUU (numero)": "",
                "Descripcion CIIU": "",
                "suma fija": None,
                "suma variable": None,
            })
            continue
        if status != "OK":
            message = str(consulta.get("message", "") or "").strip() or status or "error de consulta"
            rows.append({
                "CUIT": cuit,
                "afiliacion": message,
                "CIUU (numero)": "",
                "Descripcion CIIU": "",
                "suma fija": None,
                "suma variable": None,
            })
            continue
        if not isinstance(data, list) or not data:
            rows.append({
                "CUIT": cuit,
                "afiliacion": "sin datos de alicuota",
                "CIUU (numero)": "",
                "Descripcion CIIU": "",
                "suma fija": None,
                "suma variable": None,
            })
            continue
        for block in data:
            ciiu_num, ciiu_desc, suma_fija, suma_variable = _extract_ok_block_values(block)
            rows.append({
                "CUIT": cuit,
                "afiliacion": "",
                "CIUU (numero)": ciiu_num,
                "Descripcion CIIU": ciiu_desc,
                "suma fija": suma_fija,
                "suma variable": suma_variable,
            })
    return rows


def build_srt_excel(rows: List[Dict[str, Any]]) -> bytes:
    df = pd.DataFrame(rows)
    if df.empty:
        return b""
    ordered_columns = [
        "CUIT", "afiliacion", "CIUU (numero)", "Descripcion CIIU", "suma fija", "suma variable",
    ]
    existing = [col for col in ordered_columns if col in df.columns]
    extras = [col for col in df.columns if col not in existing]
    df = df[existing + extras]
    if "suma variable" in df.columns:
        df["suma variable"] = pd.to_numeric(df["suma variable"], errors="coerce") / 100.0
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="SRT_Consolidado")
    buf.seek(0)
    return buf.read()
