import os
import zipfile
from io import BytesIO
from typing import List, Optional

import pandas as pd

from utils.excel_utils import make_output_excel
from utils.url_utils import CUIT_REGEX


def extract_cuit_from_filename(filename: str) -> Optional[str]:
    m = CUIT_REGEX.findall(filename or "")
    if not m:
        return None
    return m[-1]


def read_csv_bytes_safely_semicolon(b: bytes) -> pd.DataFrame:
    try:
        return pd.read_csv(BytesIO(b), header=0, sep=";", dtype=str, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(BytesIO(b), header=0, sep=";", dtype=str, low_memory=False, encoding="latin-1")


def consolidate_group_from_zip(zf: zipfile.ZipFile, folder_prefix: str) -> pd.DataFrame:
    files = [n for n in zf.namelist() if n.lower().startswith(folder_prefix.lower() + "/") and n.lower().endswith(".csv")]
    dfs: List[pd.DataFrame] = []
    for name in files:
        try:
            with zf.open(name, "r") as f:
                data = f.read()
            df = read_csv_bytes_safely_semicolon(data)
            cuit = extract_cuit_from_filename(os.path.basename(name)) or ""
            df.insert(0, "Cuit", cuit)
            dfs.append(df)
        except Exception:
            continue
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, axis=0, ignore_index=True)


def build_zip_with_excels(df_emitidos: pd.DataFrame, df_recibidos: pd.DataFrame) -> bytes:
    buf_zip = BytesIO()
    with zipfile.ZipFile(buf_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
        x_emit = make_output_excel(df_emitidos if not df_emitidos.empty else pd.DataFrame(), sheet_name="Consolidados Emitidos")
        z.writestr("Consolidados Emitidos.xlsx", x_emit)
        x_rec = make_output_excel(df_recibidos if not df_recibidos.empty else pd.DataFrame(), sheet_name="Consolidados Recibidos")
        z.writestr("Consolidados Recibidos.xlsx", x_rec)
    buf_zip.seek(0)
    return buf_zip.read()
