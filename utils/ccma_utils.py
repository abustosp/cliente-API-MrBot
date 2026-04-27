import json
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from utils.parse_utils import parse_amount

CCMA_NUMERIC_FIELDS = [
    "deuda_capital",
    "deuda_accesorios",
    "total_deuda",
    "credito_capital",
    "credito_accesorios",
    "total_a_favor",
]

CCMA_MOV_COLUMNS = [
    "cuit_representante",
    "cuit_representado",
    "periodo",
    "impuesto",
    "concepto",
    "subconcepto",
    "descripcion",
    "fecha_movimiento",
    "debe",
    "haber",
]


def normalize_ccma_response(http_status: Optional[int], data: Any, cuit_rep: str, cuit_repr: str,
                            movimientos_flag: bool) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    resumen_row: Dict[str, Any] = {
        "cuit_representante": cuit_rep,
        "cuit_representado": cuit_repr,
        "http_status": http_status,
        "movimientos_solicitados": bool(movimientos_flag),
        "response_json": None,
        "error": None
    }
    movimientos_rows: List[Dict[str, Any]] = []
    if http_status == 200 and isinstance(data, dict):
        response_obj = data.get("response_ccma", data)
        status_field = data.get("status")
        error_message = data.get("error_message")
        if status_field is not None:
            resumen_row["status"] = status_field
        if error_message is not None:
            resumen_row["error_message"] = error_message
        if isinstance(response_obj, dict):
            resumen_row.update({
                "cuit": response_obj.get("cuit"),
                "periodo": response_obj.get("periodo"),
                "deuda_capital": response_obj.get("deuda_capital"),
                "deuda_accesorios": response_obj.get("deuda_accesorios"),
                "total_deuda": response_obj.get("total_deuda"),
                "credito_capital": response_obj.get("credito_capital"),
                "credito_accesorios": response_obj.get("credito_accesorios"),
                "total_a_favor": response_obj.get("total_a_favor"),
            })
            resumen_row["response_json"] = json.dumps({"response_ccma": response_obj}, ensure_ascii=False)
            for field in CCMA_NUMERIC_FIELDS:
                if field in resumen_row:
                    resumen_row[field] = parse_amount(resumen_row[field])
            if movimientos_flag:
                movimientos_list = response_obj.get("movimientos")
                if isinstance(movimientos_list, list):
                    for mov in movimientos_list:
                        if not isinstance(mov, dict):
                            continue
                        mov_row = {
                            "cuit_representante": cuit_rep,
                            "cuit_representado": cuit_repr or response_obj.get("cuit"),
                        }
                        mov_row.update(mov)
                        for monto_col in ("debe", "haber"):
                            if monto_col in mov_row:
                                mov_row[monto_col] = parse_amount(mov_row[monto_col])
                        movimientos_rows.append(mov_row)
        else:
            resumen_row["response_json"] = json.dumps(data, ensure_ascii=False)
    else:
        resumen_row["error"] = json.dumps({"http_status": http_status, "data": data}, ensure_ascii=False)
    return resumen_row, movimientos_rows


def build_ccma_outputs(resumen_rows: List[Dict[str, Any]], movimientos_rows: List[Dict[str, Any]],
                       movimientos_requested: bool) -> Tuple[pd.DataFrame, pd.DataFrame]:
    resumen_df = pd.DataFrame(resumen_rows)
    for col in CCMA_NUMERIC_FIELDS:
        if col in resumen_df.columns:
            resumen_df[col] = resumen_df[col].apply(parse_amount)
    movimientos_df = pd.DataFrame(movimientos_rows)
    if movimientos_df.empty and movimientos_requested:
        movimientos_df = pd.DataFrame(columns=CCMA_MOV_COLUMNS)
    if not movimientos_df.empty:
        mov_cols = [c for c in CCMA_MOV_COLUMNS if c in movimientos_df.columns]
        otros_cols = [c for c in movimientos_df.columns if c not in mov_cols]
        movimientos_df = movimientos_df[mov_cols + otros_cols]
        for monto_col in ("debe", "haber"):
            if monto_col in movimientos_df.columns:
                movimientos_df[monto_col] = movimientos_df[monto_col].apply(parse_amount)
    return resumen_df, movimientos_df


def build_ccma_excel(resumen_df: pd.DataFrame, movimientos_df: pd.DataFrame,
                     include_movements_sheet: bool) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        resumen_df.to_excel(writer, index=False, sheet_name="CCMA")
        if include_movements_sheet or not movimientos_df.empty:
            movimientos_df.to_excel(writer, index=False, sheet_name="Movimientos")
    buf.seek(0)
    return buf.read()
