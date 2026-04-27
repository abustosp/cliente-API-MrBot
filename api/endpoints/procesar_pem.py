import json
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from api.client import ensure_trailing_slash


def call_procesar_pem(
    pem_file_content: bytes,
    pem_filename: str,
    base_url: str,
    headers: Dict[str, str],
    timeout_sec: int = 120,
) -> Dict[str, Any]:
    endpoint = ensure_trailing_slash(base_url) + "api/v1/procesar-pem/convertir"
    try:
        response = requests.post(
            endpoint,
            headers=headers,
            files={"file": (pem_filename, pem_file_content, "application/octet-stream")},
            timeout=timeout_sec,
        )
        try:
            data = response.json()
        except Exception:
            data = {"raw_text": response.text}
        return {"http_status": response.status_code, "data": data}
    except Exception as exc:
        return {"http_status": None, "data": {"success": False, "message": f"Error de conexion: {exc}"}}


def _to_excel_cell(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def _build_pem_tables(data: Any) -> List[Tuple[str, pd.DataFrame]]:
    output: List[Tuple[str, pd.DataFrame]] = []
    if isinstance(data, dict):
        output.append(("root", pd.json_normalize(data, sep=".") if data else pd.DataFrame()))
    elif isinstance(data, list):
        output.append(("root", pd.DataFrame(data)))
    else:
        output.append(("root", pd.DataFrame([{"valor": data}])))
    return output


def build_pem_excel(response_data: Dict[str, Any]) -> bytes:
    data_for_conversion = response_data.get("datos", response_data)
    tables = _build_pem_tables(data_for_conversion)
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        metadata_df = pd.DataFrame([
            {"campo": "nombre_archivo_api", "valor": str(response_data.get("nombre_archivo", ""))},
        ])
        metadata_df.to_excel(writer, index=False, sheet_name="metadata")
        for path, df in tables:
            if df is None or df.empty:
                continue
            export_df = df.copy()
            for col in export_df.columns:
                export_df[col] = export_df[col].map(_to_excel_cell)
            sheet_name = path[:31].replace(".", "_")
            export_df.to_excel(writer, index=False, sheet_name=sheet_name)
    buf.seek(0)
    return buf.read()
