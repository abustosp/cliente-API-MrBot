from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from utils.url_utils import URL_REGEX


def make_output_excel(df: pd.DataFrame, sheet_name: str = "Consolidado") -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    buf.seek(0)
    return buf.read()


def extract_minio_urls_from_excel(uploaded_file: Any) -> Tuple[List[Dict[str, str]], pd.DataFrame]:
    df = pd.read_excel(uploaded_file, dtype=str).fillna("")
    df.columns = [c.strip().lower() for c in df.columns]
    contrib_cols = [c for c in ("cuit_representado", "representado_cuit", "contribuyente", "cuit") if c in df.columns]
    rows: List[Dict[str, str]] = []
    seen = set()
    log_rows: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        contributor_val = ""
        for col in contrib_cols:
            val = str(row.get(col, "")).strip()
            if val:
                contributor_val = val
                break
        if not contributor_val:
            contributor_val = "sin_identificar"

        for val in row.to_list():
            text = str(val) if val is not None else ""
            if not text or text.strip().lower() in {"nan", "none"}:
                continue
            for m in URL_REGEX.findall(text):
                url = m.strip()
                if not url:
                    continue
                if "minio" not in url.lower():
                    log_rows.append({"contribuyente": contributor_val, "url": url, "estado": "ignorado_no_minio"})
                    continue
                key = (contributor_val, url)
                if key in seen:
                    continue
                seen.add(key)
                rows.append({"contribuyente": contributor_val, "url": url})
                log_rows.append({"contribuyente": contributor_val, "url": url, "estado": "ok"})
    return rows, pd.DataFrame(log_rows)


def collect_url_entries_from_df(
    df: pd.DataFrame,
    url_col: Optional[str],
    contributor_col: Optional[str],
    extract_zip: bool
) -> List[Dict[str, Any]]:
    from utils.parse_utils import normalize_contributor_id
    entries: List[Dict[str, Any]] = []
    if not url_col or url_col not in df.columns:
        return entries
    seen = set()
    for _, row in df.iterrows():
        contrib_val = ""
        if contributor_col and contributor_col in df.columns:
            contrib_val = normalize_contributor_id(row.get(contributor_col))
        cell = row.get(url_col, "")
        text = str(cell) if cell is not None else ""
        if not text or text.strip().lower() in {"nan", "none"}:
            continue
        for m in URL_REGEX.findall(text):
            url = m.strip()
            if not url:
                continue
            key = (contrib_val, url)
            if key in seen:
                continue
            seen.add(key)
            entry: Dict[str, Any] = {"url": url, "extract": extract_zip}
            if contrib_val:
                entry["contribuyente"] = contrib_val
            entries.append(entry)
    return entries
