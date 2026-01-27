# app_mis_comprobantes_tabs.py
import streamlit as st
import pandas as pd
import requests
from io import BytesIO
from datetime import date
from typing import Any, Dict, Optional, Tuple, List
from urllib.parse import urlparse, unquote
import os
import re
import zipfile
import base64  # Para decodificar archivos en base64 cuando sea necesario
import json  # Para serializar datos al generar Excel con resultados masivos

# Cargar variables de entorno desde un archivo `.env` si existe.
# Esto permite precargar valores sensibles como la API key y el email.
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    # Si dotenv no está instalado o falla, no interrumpir la ejecución.
    pass

# =========================
# CONFIGURACIÓN BÁSICA UI
# =========================
st.set_page_config(
    page_title="BOTs de Mrbot",
    page_icon="static/ABP.png",
    layout="wide")
st.title("BOTs de Mrbot")
st.caption("Consultas masivas, estado de consultas, descarga desde S3/MinIO y consolidación final de archivos.")

# =========================
# PARÁMETROS GLOBALES (Sidebar)
# =========================
with st.sidebar:
    st.header("⚙️ Conexión")
    # Leer valores por defecto de variables de entorno si están definidos en un archivo .env
    default_api_key = os.getenv("X_API_KEY", "")
    default_email = os.getenv("EMAIL", "")
    base_url = st.text_input(
        "Base URL de la API",
        value="https://api-bots.mrbot.com.ar/",
        help="Ej.: https://api-bots.mrbot.com.ar/ (debe terminar con /)"
    )
    # La API key y el email se precargan con valores de entorno (si existen) pero pueden ser editados por el usuario.
    x_api_key = st.text_input("x-api-key (opcional, header)", value=default_api_key, type="password")
    header_email = st.text_input("email (opcional, header)", value=default_email)

# =========================
# UTILIDADES COMUNES
# =========================
REQUIRED_COLS = ["cuit_inicio_sesion", "nombre_representado", "cuit_representado", "contrasena"]

def ensure_trailing_slash(url: str) -> str:
    return url if url.endswith("/") else url + "/"

def as_ddmmyyyy(d: date) -> str:
    return d.strftime("%d/%m/%Y")

def build_headers(x_api_key: Optional[str], email: Optional[str]) -> Dict[str, str]:
    headers: Dict[str, str] = {"Content-Type": "application/json"}
    if x_api_key:
        headers["x-api-key"] = x_api_key
    if email:
        headers["email"] = email
    return headers

def call_consulta(base_url: str, headers: Dict[str, str], payload: Dict[str, Any],
                  timeout_sec: int = 120, max_retries: int = 2) -> Dict[str, Any]:
    url = ensure_trailing_slash(base_url) + "api/v1/mis_comprobantes/consulta"
    last_exc: Optional[Exception] = None
    for _ in range(max_retries + 1):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=timeout_sec)
            try:
                data = resp.json()
            except Exception:
                data = {"raw_text": resp.text}
            return {"http_status": resp.status_code, "data": data}
        except Exception as e:
            last_exc = e
    return {"http_status": None, "data": {"success": False, "message": f"Error de conexión: {last_exc}"}}

def call_consultas_disponibles(base_url: str, email_path: str, headers: Dict[str, str],
                               timeout_sec: int = 60) -> Tuple[Optional[int], Optional[Dict[str, Any]], Optional[int], Optional[str]]:
    url = ensure_trailing_slash(base_url) + f"api/v1/user/consultas/{email_path}"
    try:
        resp = requests.get(url, headers=headers, timeout=timeout_sec)
        js = None
        try:
            js = resp.json()
        except Exception:
            return resp.status_code, None, None, f"Respuesta no-JSON: {resp.text[:500]}"
        cd = js.get("consultas_disponibles") if isinstance(js, dict) else None
        return resp.status_code, js, cd, None
    except Exception as e:
        return None, None, None, f"Error de conexión: {e}"

# -------------------------------------------------------------
# UTILIDADES CCMA (parseo de montos, movimientos y Excel)
# -------------------------------------------------------------
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


def parse_bool_cell(value: Any, default: bool = False) -> bool:
    """
    Normaliza valores provenientes de Excel (sí/no, 1/0, true/false).
    Si no coincide con ningún valor conocido, retorna el default.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text == "":
        return default
    if text in {"1", "true", "t", "yes", "y", "si", "sí", "s"}:
        return True
    if text in {"0", "false", "f", "no", "n"}:
        return False
    return default


def parse_amount(value: Any) -> Optional[float]:
    """
    Convierte strings con separador de miles y decimal a float.
    Admite formatos tipo 22,307.22 (coma miles, punto decimal) y 22.307,22.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace("\xa0", "").replace(" ", "")
    if text == "":
        return None
    try:
        if "," in text and "." in text:
            if text.rfind(".") > text.rfind(","):
                text = text.replace(",", "")
            else:
                text = text.replace(".", "").replace(",", ".")
        elif "," in text:
            text = text.replace(".", "").replace(",", ".")
        return float(text)
    except Exception:
        return None


def normalize_ccma_response(http_status: Optional[int], data: Any, cuit_rep: str, cuit_repr: str,
                            movimientos_flag: bool) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Extrae campos útiles del response de CCMA y opcionalmente los movimientos.
    Devuelve (row_resumen, lista_movimientos).
    """
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

# -------------------------------------------------------------------
# NUEVAS FUNCIONES DE LLAMADA PARA OTROS ENDPOINTS
# Estas funciones encapsulan las llamadas HTTP a los distintos servicios disponibles
# en la API de bots (Comprobantes en Línea, SCT, CCMA, Apócrifos y Consulta de CUIT).

def call_rcel_consulta(base_url: str, headers: Dict[str, str], payload: Dict[str, Any], timeout_sec: int = 120) -> Dict[str, Any]:
    """
    Consulta el endpoint Comprobantes en Línea (/api/v1/rcel/consulta).
    Retorna el status HTTP y el JSON recibido (o texto sin procesar en caso de error de decodificación).
    """
    url = ensure_trailing_slash(base_url) + "api/v1/rcel/consulta"
    last_exc: Optional[Exception] = None
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout_sec)
        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text}
        return {"http_status": resp.status_code, "data": data}
    except Exception as e:
        last_exc = e
        return {"http_status": None, "data": {"success": False, "message": f"Error de conexión: {last_exc}"}}

def call_sct_consulta(base_url: str, headers: Dict[str, str], payload: Dict[str, Any], timeout_sec: int = 120) -> Dict[str, Any]:
    """
    Consulta el endpoint Sistema de Cuentas Tributarias (/api/v1/sct/consulta).
    Retorna el status HTTP y el JSON recibido (o texto sin procesar en caso de error de decodificación).
    """
    url = ensure_trailing_slash(base_url) + "api/v1/sct/consulta"
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout_sec)
        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text}
        return {"http_status": resp.status_code, "data": data}
    except Exception as e:
        return {"http_status": None, "data": {"success": False, "message": f"Error de conexión: {e}"}}

def call_ccma_consulta(base_url: str, headers: Dict[str, str], payload: Dict[str, Any], timeout_sec: int = 120) -> Dict[str, Any]:
    """
    Consulta el endpoint Cuenta Corriente de Monotributistas y Autónomos (/api/v1/ccma/consulta).
    Retorna el status HTTP y el JSON recibido (o texto sin procesar en caso de error de decodificación).
    """
    url = ensure_trailing_slash(base_url) + "api/v1/ccma/consulta"
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout_sec)
        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text}
        return {"http_status": resp.status_code, "data": data}
    except Exception as e:
        return {"http_status": None, "data": {"success": False, "message": f"Error de conexión: {e}"}}

def call_apoc_consulta(base_url: str, headers: Dict[str, str], cuit: str, timeout_sec: int = 60) -> Dict[str, Any]:
    """
    Consulta si un CUIT está en la base de apócrifos (/api/v1/apoc/consulta/{cuit}).
    Retorna el status HTTP y el JSON recibido (o texto sin procesar en caso de error de decodificación).
    """
    url = ensure_trailing_slash(base_url) + f"api/v1/apoc/consulta/{cuit}"
    try:
        resp = requests.get(url, headers=headers, timeout=timeout_sec)
        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text}
        return {"http_status": resp.status_code, "data": data}
    except Exception as e:
        return {"http_status": None, "data": {"success": False, "message": f"Error de conexión: {e}"}}

def call_cuit_individual(base_url: str, headers: Dict[str, str], payload: Dict[str, Any], timeout_sec: int = 60) -> Dict[str, Any]:
    """
    Consulta la constancia de inscripción de un CUIT individual (/api/v1/consulta_cuit/individual).
    Retorna el status HTTP y el JSON recibido.
    """
    url = ensure_trailing_slash(base_url) + "api/v1/consulta_cuit/individual"
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout_sec)
        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text}
        return {"http_status": resp.status_code, "data": data}
    except Exception as e:
        return {"http_status": None, "data": {"success": False, "message": f"Error de conexión: {e}"}}

def call_cuit_masivo(base_url: str, headers: Dict[str, str], payload: Dict[str, Any], timeout_sec: int = 120) -> Dict[str, Any]:
    """
    Consulta la constancia de inscripción de múltiples CUITs (/api/v1/consulta_cuit/masivo).
    Retorna el status HTTP y el JSON recibido.
    """
    url = ensure_trailing_slash(base_url) + "api/v1/consulta_cuit/masivo"
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout_sec)
        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text}
        return {"http_status": resp.status_code, "data": data}
    except Exception as e:
        return {"http_status": None, "data": {"success": False, "message": f"Error de conexión: {e}"}}

def call_mis_retenciones_consulta(base_url: str, headers: Dict[str, str], payload: Dict[str, Any], timeout_sec: int = 120) -> Dict[str, Any]:
    """
    Consulta el endpoint Mis Retenciones (/api/v1/mis_retenciones/consulta).
    Retorna el status HTTP y el JSON recibido.
    """
    url = ensure_trailing_slash(base_url) + "api/v1/mis_retenciones/consulta"
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout_sec)
        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text}
        return {"http_status": resp.status_code, "data": data}
    except Exception as e:
        return {"http_status": None, "data": {"success": False, "message": f"Error de conexión: {e}"}}

def call_sifere_consulta(base_url: str, headers: Dict[str, str], payload: Dict[str, Any], timeout_sec: int = 600) -> Dict[str, Any]:
    """
    Consulta el endpoint SIFERE (/api/v1/sifere/consulta).
    Retorna el status HTTP y el JSON recibido.
    """
    url = ensure_trailing_slash(base_url) + "api/v1/sifere/consulta"
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout_sec)
        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text}
        return {"http_status": resp.status_code, "data": data}
    except Exception as e:
        return {"http_status": None, "data": {"success": False, "message": f"Error de conexión: {e}"}}

def call_declaracion_en_linea_consulta(base_url: str, headers: Dict[str, str], payload: Dict[str, Any], timeout_sec: int = 120) -> Dict[str, Any]:
    """
    Consulta el endpoint Declaración en Línea (/api/v1/declaracion-en-linea/consulta).
    Retorna el status HTTP y el JSON recibido.
    """
    url = ensure_trailing_slash(base_url) + "api/v1/declaracion-en-linea/consulta"
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout_sec)
        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text}
        return {"http_status": resp.status_code, "data": data}
    except Exception as e:
        return {"http_status": None, "data": {"success": False, "message": f"Error de conexión: {e}"}}

def call_mis_facilidades_consulta(base_url: str, headers: Dict[str, str], payload: Dict[str, Any], timeout_sec: int = 600) -> Dict[str, Any]:
    """
    Consulta el endpoint Mis Facilidades (/api/v1/mis_facilidades/consulta).
    Retorna el status HTTP y el JSON recibido.
    """
    url = ensure_trailing_slash(base_url) + "api/v1/mis_facilidades/consulta"
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout_sec)
        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text}
        return {"http_status": resp.status_code, "data": data}
    except Exception as e:
        return {"http_status": None, "data": {"success": False, "message": f"Error de conexión: {e}"}}

def call_aportes_en_linea_consulta(base_url: str, headers: Dict[str, str], payload: Dict[str, Any], timeout_sec: int = 120) -> Dict[str, Any]:
    """
    Consulta el endpoint Aportes en Línea (/api/v1/aportes-en-linea/consulta).
    Retorna el status HTTP y el JSON recibido.
    """
    url = ensure_trailing_slash(base_url) + "api/v1/aportes-en-linea/consulta"
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout_sec)
        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text}
        return {"http_status": resp.status_code, "data": data}
    except Exception as e:
        return {"http_status": None, "data": {"success": False, "message": f"Error de conexión: {e}"}}

# -------------------------------------------------------------
# NUEVAS FUNCIONES PARA GESTIÓN DE USUARIOS
# Estas funciones encapsulan la creación de usuarios y el reseteo de la API key.

def call_create_user_api(base_url: str, payload: Dict[str, Any], timeout_sec: int = 60) -> Dict[str, Any]:
    """
    Crea un nuevo usuario enviando un correo con la API key.

    Según la especificación de la API, el endpoint
    `/api/v1/user/` recibe un cuerpo JSON con un único campo `mail`
    (la dirección de correo del nuevo usuario). Si se envían claves
    adicionales (por ejemplo, `email`), el servidor podría ignorarlas o
    devolver un error de validación. Por ello se recomienda construir
    `payload` como `{"mail": "usuario@example.com"}`.
    """
    url = ensure_trailing_slash(base_url) + "api/v1/user/"
    try:
        resp = requests.post(url, json=payload, timeout=timeout_sec)
        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text}
        return {"http_status": resp.status_code, "data": data}
    except Exception as e:
        return {"http_status": None, "data": {"success": False, "message": f"Error de conexión: {e}"}}

def call_reset_api_key(base_url: str, payload: Dict[str, Any], timeout_sec: int = 60) -> Dict[str, Any]:
    """
    Resetea la API key de un usuario y envía la nueva clave por correo.

    De acuerdo con la especificación OpenAPI de https://api-bots.mrbot.com.ar/, el
    endpoint `/api/v1/user/reset-key/` acepta el correo electrónico como parámetro
    de consulta (`email`) y **no** espera un cuerpo JSON. La implementación
    anterior enviaba el correo en el JSON, lo cual hacía que el servidor
    respondiera con un error de validación. Esta versión extrae la dirección de
    correo del diccionario `payload` (aceptando tanto la clave `mail` como
    `email`) y la envía en la cadena de consulta.

    :param base_url: URL base de la API, por ejemplo "https://api-bots.mrbot.com.ar/".
    :param payload: Diccionario con la dirección de correo del usuario. Puede
        contener la clave "mail" o "email".
    :param timeout_sec: Tiempo máximo de espera para la solicitud.
    :return: Un diccionario con el código de estado HTTP y los datos devueltos
        por el servidor.
    """
    url = ensure_trailing_slash(base_url) + "api/v1/user/reset-key/"
    # Extraer el email desde el payload. Algunos formularios utilizan la clave
    # "mail" y otros "email"; se soportan ambos.
    email_param = None
    if isinstance(payload, dict):
        email_param = payload.get("email") or payload.get("mail")
    # Construir parámetros de consulta sólo si se proporciona un correo.
    params: Optional[Dict[str, str]] = {"email": email_param} if email_param else None
    try:
        # No se envía un cuerpo JSON; solo parámetros de consulta.
        resp = requests.post(url, params=params, timeout=timeout_sec)
        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text}
        return {"http_status": resp.status_code, "data": data}
    except Exception as e:
        return {"http_status": None, "data": {"success": False, "message": f"Error de conexión: {e}"}}

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

def make_output_excel(df: pd.DataFrame, sheet_name: str = "Consolidado") -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    buf.seek(0)
    return buf.read()

def sanitize_filename(name: str) -> str:
    name = unquote(name)
    name = re.sub(r"[\\/*?\"<>|:#]", "_", name)
    name = name.strip().strip(".")
    return name or "archivo"

def infer_filename_from_url(url: str) -> str:
    try:
        path = urlparse(url).path
        base = os.path.basename(path)
        if base:
            return sanitize_filename(base)
    except Exception:
        pass
    return "archivo"

def get_filename_from_headers(resp: requests.Response) -> Optional[str]:
    cd = resp.headers.get("Content-Disposition")
    if not cd:
        return None
    m = re.search(r'filename\*=UTF-8\'\'(.+)', cd)
    if m:
        return sanitize_filename(unquote(m.group(1)))
    m = re.search(r'filename="?([^"]+)"?', cd)
    if m:
        return sanitize_filename(m.group(1))
    return None

def normalize_contributor_id(value: Any) -> str:
    text = str(value).strip() if value is not None else ""
    return text if text else "sin_identificar"

def is_zip_bytes(b: bytes, content_type: Optional[str], fallback_name: Optional[str]) -> bool:
    if content_type and "zip" in content_type.lower():
        return True
    if fallback_name and fallback_name.lower().endswith(".zip"):
        return True
    return b.startswith(b"PK\x03\x04")

def write_unique(zf: zipfile.ZipFile, arcname: str, data: bytes) -> str:
    base_dir, fname = os.path.split(arcname)
    base, ext = os.path.splitext(fname)
    candidate = arcname
    k = 1
    while candidate in zf.namelist():
        candidate = os.path.join(base_dir, f"{base}_{k}{ext}")
        k += 1
    zf.writestr(candidate, data)
    return candidate

def download_to_zip(
    urls_emitidos: List[Any],
    urls_recibidos: List[Any],
    timeout_sec: int = 120,
    extract_zips: bool = True
) -> Tuple[bytes, pd.DataFrame]:
    log_rows = []
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        def process_list(urls, carpeta, tipo):
            for item in urls:
                url = ""
                contrib = ""
                extract_flag = extract_zips
                if isinstance(item, dict):
                    url = str(item.get("url") or "")
                    contrib = str(item.get("contribuyente") or "")
                    if "extract" in item:
                        extract_flag = bool(item.get("extract"))
                elif isinstance(item, str):
                    url = item
                if not url:
                    continue
                try:
                    r = requests.get(url, timeout=timeout_sec, stream=True)
                    if r.status_code != 200:
                        log_rows.append({
                            "tipo": tipo,
                            "contribuyente": contrib,
                            "url": url,
                            "estado": "error_http",
                            "detalle": f"HTTP {r.status_code}"
                        })
                        continue
                    fname = get_filename_from_headers(r) or infer_filename_from_url(url)
                    ctype = r.headers.get("Content-Type", "")
                    content = r.content
                    target_dir = carpeta
                    if contrib:
                        contrib_id = normalize_contributor_id(contrib)
                        target_dir = os.path.join(carpeta, sanitize_filename(contrib_id))
                    if extract_flag and is_zip_bytes(content, ctype, fname):
                        try:
                            with zipfile.ZipFile(BytesIO(content)) as inzip:
                                had_file = False
                                for zi in inzip.infolist():
                                    if zi.is_dir():
                                        continue
                                    try:
                                        raw = inzip.read(zi.filename)
                                    except Exception as e:
                                        log_rows.append({
                                            "tipo": tipo,
                                            "contribuyente": contrib,
                                            "url": url,
                                            "estado": "error_lectura_zip",
                                            "detalle": f"{zi.filename}: {e}"
                                        })
                                        continue
                                    inner_name = sanitize_filename(os.path.basename(zi.filename)) or "archivo"
                                    arcname = os.path.join(target_dir, inner_name)
                                    final_name = write_unique(zf, arcname, raw)
                                    had_file = True
                                    log_rows.append({
                                        "tipo": tipo,
                                        "contribuyente": contrib,
                                        "url": url,
                                        "estado": "ok_extraido",
                                        "detalle": final_name
                                    })
                                if not had_file:
                                    log_rows.append({
                                        "tipo": tipo,
                                        "contribuyente": contrib,
                                        "url": url,
                                        "estado": "zip_vacio",
                                        "detalle": fname
                                    })
                        except zipfile.BadZipFile:
                            arcname = os.path.join(target_dir, fname or "archivo")
                            final_name = write_unique(zf, arcname, content)
                            log_rows.append({
                                "tipo": tipo,
                                "contribuyente": contrib,
                                "url": url,
                                "estado": "ok_archivo",
                                "detalle": final_name
                            })
                    else:
                        arcname = os.path.join(target_dir, fname or "archivo")
                        final_name = write_unique(zf, arcname, content)
                        log_rows.append({
                            "tipo": tipo,
                            "contribuyente": contrib,
                            "url": url,
                            "estado": "ok_archivo",
                            "detalle": final_name
                        })
                except Exception as e:
                    log_rows.append({
                        "tipo": tipo,
                        "contribuyente": contrib,
                        "url": url,
                        "estado": "error",
                        "detalle": str(e)
                    })
        process_list(urls_emitidos, "Emitidos", "emitido")
        process_list(urls_recibidos, "Recibidos", "recibido")
    zip_buffer.seek(0)
    return zip_buffer.read(), pd.DataFrame(log_rows)

# =========================
# FUNCIÓN GENÉRICA PARA DESCARGA DE ARCHIVOS MinIO POR CONTRIBUYENTE
# =========================
def download_minio_to_zip_by_contributor(
    data_rows: List[Dict[str, Any]],
    url_field: str,
    contributor_field: str,
    timeout_sec: int = 120
) -> Tuple[bytes, pd.DataFrame]:
    """
    Descarga archivos desde URLs de MinIO y los organiza en un ZIP con carpetas por contribuyente.
    No extrae ZIPs internos: guarda cada archivo con su nombre original.

    Args:
        data_rows: Lista de diccionarios con los datos (ej: resultado de consultas)
        url_field: Nombre del campo que contiene la URL de MinIO (ej: 'url_minio', 'data')
        contributor_field: Nombre del campo identificador del contribuyente (ej: 'cuit_representado', 'cuit_login')
        timeout_sec: Timeout para descargas HTTP

    Returns:
        Tupla con (bytes del ZIP, DataFrame con log de operaciones)
    """
    log_rows = []
    zip_buffer = BytesIO()
    
    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for row in data_rows:
            contributor_id = normalize_contributor_id(row.get(contributor_field))
            
            # Extraer URLs desde el campo especificado
            url_value = row.get(url_field)
            urls_to_process = []
            seen_urls = set()

            def add_url(u: str) -> None:
                if u and u not in seen_urls:
                    seen_urls.add(u)
                    urls_to_process.append(u)

            def collect_urls(obj: Any) -> None:
                if obj is None:
                    return
                if isinstance(obj, str):
                    for m in re.findall(r"https?://[^\s\"'<>]+", obj):
                        add_url(m)
                    return
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        # detectar URLs directas en claves típicas
                        if isinstance(v, str):
                            collect_urls(v)
                        else:
                            collect_urls(v)
                    return
                if isinstance(obj, list):
                    for item in obj:
                        collect_urls(item)
                    return
            
            # Si el campo contiene un JSON string, parsearlo y extraer URLs
            if isinstance(url_value, str):
                try:
                    parsed = json.loads(url_value)
                    collect_urls(parsed)
                except Exception:
                    collect_urls(url_value)
            else:
                collect_urls(url_value)
            
            # Procesar cada URL encontrada
            for url in urls_to_process:
                if not url:
                    continue
                try:
                    r = requests.get(url, timeout=timeout_sec, stream=True)
                    if r.status_code != 200:
                        log_rows.append({
                            "contribuyente": contributor_id,
                            "url": url,
                            "estado": "error_http",
                            "detalle": f"HTTP {r.status_code}"
                        })
                        continue
                    
                    fname = get_filename_from_headers(r) or infer_filename_from_url(url)
                    content = r.content

                    # Carpeta del contribuyente
                    carpeta = sanitize_filename(str(contributor_id))

                    arcname = os.path.join(carpeta, fname or "archivo")
                    final_name = write_unique(zf, arcname, content)
                    log_rows.append({
                        "contribuyente": contributor_id,
                        "url": url,
                        "estado": "ok_archivo",
                        "detalle": final_name
                    })
                except Exception as e:
                    log_rows.append({
                        "contribuyente": contributor_id,
                        "url": url,
                        "estado": "error",
                        "detalle": str(e)
                    })
    
    zip_buffer.seek(0)
    return zip_buffer.read(), pd.DataFrame(log_rows)

# =========================
# NUEVAS UTILIDADES — SOLAPA 4
# =========================
URL_REGEX = re.compile(r"https?://[^\s\"'<>]+")
CUIT_REGEX = re.compile(r"(?<!\d)(\d{11})(?!\d)")

def extract_minio_urls_from_excel(uploaded_file: Any) -> Tuple[List[Dict[str, str]], pd.DataFrame]:
    """
    Lee un Excel y extrae URLs de MinIO desde cualquier celda.
    Si existe una columna de contribuyente (p.ej. cuit_representado),
    se asocia cada URL a ese contribuyente para armar subcarpetas.
    """
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

def download_minio_links_to_zip(
    urls: List[str],
    folder: str = "MinIO",
    timeout_sec: int = 120
) -> Tuple[bytes, pd.DataFrame]:
    """
    Descarga una lista de URLs de MinIO y arma un ZIP.
    Usa el nombre original del archivo. No extrae ZIPs internos.
    """
    log_rows = []
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for url in urls:
            if not url:
                continue
            try:
                r = requests.get(url, timeout=timeout_sec, stream=True)
                if r.status_code != 200:
                    log_rows.append({"url": url, "estado": "error_http", "detalle": f"HTTP {r.status_code}"})
                    continue
                fname = get_filename_from_headers(r) or infer_filename_from_url(url)
                content = r.content
                carpeta = sanitize_filename(folder)
                arcname = os.path.join(carpeta, fname or "archivo")
                final_name = write_unique(zf, arcname, content)
                log_rows.append({"url": url, "estado": "ok_archivo", "detalle": final_name})
            except Exception as e:
                log_rows.append({"url": url, "estado": "error", "detalle": str(e)})
    zip_buffer.seek(0)
    return zip_buffer.read(), pd.DataFrame(log_rows)

def render_minio_mass_download(section_title: str, uploader_key: str, timeout_key: str, button_key: str) -> None:
    st.markdown(f"#### Descarga masiva {section_title} (solo links MinIO)")
    st.write(
        "Subí un Excel con links MinIO. Si hay columna de contribuyente (ej: `cuit_representado`), "
        "se crearán subcarpetas por contribuyente."
    )
    up = st.file_uploader("Archivo Excel (.xlsx)", type=["xlsx"], key=uploader_key)
    timeout = st.number_input("Timeout por archivo (segundos)", min_value=10, value=120, step=10, key=timeout_key)
    if up is not None:
        try:
            rows, scan_log = extract_minio_urls_from_excel(up)
        except Exception as e:
            st.error(f"Error leyendo el Excel: {e}")
            return
        st.write(f"Links MinIO detectados: {len(rows)}")
        if st.button("📦 Descargar ZIP desde MinIO", key=button_key):
            if not rows:
                st.warning("No se encontraron links MinIO.")
                return
            with st.spinner("Descargando archivos desde MinIO..."):
                zip_bytes, log_df = download_minio_to_zip_by_contributor(
                    rows,
                    url_field="url",
                    contributor_field="contribuyente",
                    timeout_sec=int(timeout)
                )
            st.download_button(
                label="⬇️ Descargar ZIP",
                data=zip_bytes,
                file_name=f"{section_title.lower().replace(' ', '_')}_minio_{date.today().strftime('%Y%m%d')}.zip",
                mime="application/zip",
                key=f"download_zip_{button_key}"
            )
            log_xlsx = make_output_excel(log_df, sheet_name="Log_Descargas")
            st.download_button(
                label="📋 Descargar Log",
                data=log_xlsx,
                file_name=f"log_{section_title.lower().replace(' ', '_')}_{date.today().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"download_log_{button_key}"
            )

def extract_cuit_from_filename(filename: str) -> Optional[str]:
    m = CUIT_REGEX.findall(filename or "")
    if not m:
        return None
    return m[-1]

def read_csv_bytes_safely_semicolon(b: bytes) -> pd.DataFrame:
    """
    Lee CSV con separador ';'. Intenta UTF-8 y, si falla, Latin-1.
    Header en la primera fila.
    """
    try:
        return pd.read_csv(BytesIO(b), header=0, sep=";", dtype=str, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(BytesIO(b), header=0, sep=";", dtype=str, low_memory=False, encoding="latin-1")

def consolidate_group_from_zip(zf: zipfile.ZipFile, folder_prefix: str) -> pd.DataFrame:
    """
    Lee todos los CSV dentro de `folder_prefix/` (p.ej. 'Emitidos' o 'Recibidos'),
    usa la primera fila como encabezado, apila todo y agrega primera columna 'Cuit'
    extraída del nombre de archivo.
    """
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
            # Si un archivo falla, se omite silenciosamente para no cortar el flujo
            continue
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, axis=0, ignore_index=True)

def build_zip_with_excels(df_emitidos: pd.DataFrame, df_recibidos: pd.DataFrame) -> bytes:
    """
    Construye un ZIP en memoria con:
      - Consolidados Emitidos.xlsx
      - Consolidados Recibidos.xlsx
    """
    buf_zip = BytesIO()
    with zipfile.ZipFile(buf_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
        # Emitidos
        x_emit = make_output_excel(df_emitidos if not df_emitidos.empty else pd.DataFrame(), sheet_name="Consolidados Emitidos")
        z.writestr("Consolidados Emitidos.xlsx", x_emit)
        # Recibidos
        x_rec = make_output_excel(df_recibidos if not df_recibidos.empty else pd.DataFrame(), sheet_name="Consolidados Recibidos")
        z.writestr("Consolidados Recibidos.xlsx", x_rec)
    buf_zip.seek(0)
    return buf_zip.read()

# =========================
# TABS (nueva estructura)
# =========================
# Definir las solapas principales en el nuevo orden solicitado.
(tab_users, tab_mis_comprobantes, tab_rcel, tab_sct, tab_ccma, tab_mis_retenciones, 
 tab_sifere, tab_declaracion_linea, tab_mis_facilidades, tab_aportes_linea, 
 tab_apoc, tab_cuit) = st.tabs([
    "Usuarios",
    "Mis Comprobantes",
    "RCEL",
    "SCT",
    "CCMA",
    "Mis Retenciones",
    "SIFERE",
    "Declaración en Línea",
    "Mis Facilidades",
    "Aportes en Línea",
    "APOC",
    "Consulta de CUIT"
])

# Crear sub-solapas dentro de "Mis Comprobantes" que corresponden a las funcionalidades
# de consulta masiva, descarga de archivos y consolidación de salidas.
with tab_mis_comprobantes:
    subtab_mc_consulta, subtab_mc_descarga_zip, subtab_mc_consolidar = st.tabs([
        "Consulta masiva",
        "Descargar ZIP",
        "Consolidar salidas"
    ])

# Crear sub-solapas dentro de "Usuarios" para crear usuarios, resetear la API key
# y consultar la cantidad de consultas disponibles. La tercera solapa reutilizará
# el código existente que consultaba las consultas disponibles.
with tab_users:
    subtab_user_create, subtab_user_reset, subtab_user_consultas = st.tabs([
        "Crear usuario",
        "Resetear API key",
        "Consultas disponibles"
    ])

# Asignar las variables utilizadas previamente a las nuevas sub-tabs para que el
# resto del código (definido más abajo) siga funcionando sin cambios de indentación.
tab1 = subtab_mc_consulta
tab2 = subtab_user_consultas
tab3 = subtab_mc_descarga_zip
tab4 = subtab_mc_consolidar
# tab5 ya no existe (antes agrupaba otros endpoints). Se mantienen referencias más abajo directamente a cada tab específico.
# Si se requiere una agrupación futura, crear una lista o dict.
# Eliminado: tab5 = tab_other

# Contenido adicional para las solapas de Usuarios (crear y resetear usuarios).
with subtab_user_create:
    st.subheader("Crear usuario")
    st.write("Crear un nuevo usuario y enviarle la API key por correo.")
    user_email_create = st.text_input("Email para crear usuario", value="", key="create_user_email")
    if st.button("Crear usuario", key="btn_create_user"):
        if not user_email_create.strip():
            st.warning("Ingresá un email válido.")
        else:
            payload_create = {"mail": user_email_create.strip()}
            with st.spinner("Creando usuario..."):
                resp_create = call_create_user_api(base_url, payload_create)
            st.info(f"HTTP status: {resp_create.get('http_status')}")
            st.json(resp_create.get('data'))

with subtab_user_reset:
    st.subheader("Resetear API key")
    st.write("Restablece la API key de un usuario y envía la nueva clave por correo.")
    user_email_reset = st.text_input("Email para resetear API key", value="", key="reset_user_email")
    if st.button("Resetear API key", key="btn_reset_api_key"):
        if not user_email_reset.strip():
            st.warning("Ingresá un email válido.")
        else:
            payload_reset = {"mail": user_email_reset.strip()}
            with st.spinner("Reseteando API key..."):
                resp_reset = call_reset_api_key(base_url, payload_reset)
            st.info(f"HTTP status: {resp_reset.get('http_status')}")
            st.json(resp_reset.get('data'))

# -------------------------------------------------------------------
# TAB 1: Consulta Masiva
# -------------------------------------------------------------------
with tab1:
    st.subheader("1) Consulta masiva a /api/v1/mis_comprobantes/consulta")
    with st.expander("📅 Parámetros de consulta", expanded=True):
        col_d1, col_d2 = st.columns(2)
        with col_d1:
            desde = st.date_input("Desde", value=date(date.today().year, 1, 1), format="DD/MM/YYYY")
        with col_d2:
            hasta = st.date_input("Hasta", value=date.today(), format="DD/MM/YYYY")

        col_opts1, col_opts2, col_opts3 = st.columns(3)
        with col_opts1:
            descarga_emitidos = st.checkbox("Descargar emitidos", value=True)
        with col_opts2:
            descarga_recibidos = st.checkbox("Descargar recibidos", value=True)
        with col_opts3:
            proxy_request = st.toggle("Usar proxy_request", value=False, help="Se envía como booleano en el body")

        # Nuevas opciones de la API: elegir dónde subir los archivos y recibir la respuesta en JSON.
        st.caption("Opciones de carga de archivos (API v1)")
        col_c1, col_c2, col_c3 = st.columns(3)
        with col_c1:
            carga_s3 = st.checkbox("Subir a S3", value=False)
        with col_c2:
            carga_minio = st.checkbox("Subir a MinIO", value=True)
        with col_c3:
            carga_json = st.checkbox("Recibir JSON", value=False, help="Devuelve arrays JSON en la respuesta")

        st.caption("Los archivos no se envían en base64 (`b64 = False`), y las opciones seleccionadas determinarán el tipo de salida.")

    st.markdown("### 📤 Cargar archivo Excel (credenciales por representado)")
    st.write("El Excel debe contener exactamente estas columnas:")
    st.code("cuit_inicio_sesion, nombre_representado, cuit_representado, contrasena", language="text")

    uploaded = st.file_uploader("Seleccioná el archivo .xlsx", type=["xlsx"], key="uploader_tab1")

    if uploaded is not None:
        try:
            input_df = pd.read_excel(uploaded, dtype=str).fillna("")
        except Exception as e:
            st.error(f"Error leyendo el Excel: {e}")
            st.stop()

        input_df.columns = [c.strip().lower() for c in input_df.columns]
        missing = [c for c in REQUIRED_COLS if c not in input_df.columns]
        if missing:
            st.error(f"Faltan columnas requeridas: {', '.join(missing)}")
            st.stop()

        input_df = input_df[
            (input_df["cuit_inicio_sesion"].str.strip() != "") &
            (input_df["nombre_representado"].str.strip() != "") &
            (input_df["cuit_representado"].str.strip() != "") &
            (input_df["contrasena"].str.strip() != "")
        ].copy()

        st.success(f"Archivo cargado correctamente. Filas a procesar: {len(input_df)}")

        with st.expander("👀 Vista previa (primeras filas)"):
            st.dataframe(input_df.head(10), use_container_width=True)

        if st.button("🚀 Procesar consultas y generar consolidado", key="procesar_tab1"):
            if len(input_df) == 0:
                st.warning("No hay filas válidas para procesar.")
                st.stop()

            headers = build_headers(x_api_key, header_email)
            out_rows = []
            progress = st.progress(0)
            status_ph = st.empty()

            for idx, row in input_df.reset_index(drop=True).iterrows():
                status_ph.info(
                    f"Procesando {idx+1}/{len(input_df)} — {row['nombre_representado']} (CUIT {row['cuit_representado']})"
                )
                payload = {
                    "desde": as_ddmmyyyy(desde),
                    "hasta": as_ddmmyyyy(hasta),
                    "cuit_inicio_sesion": row["cuit_inicio_sesion"].strip(),
                    "representado_nombre": row["nombre_representado"].strip(),
                    "representado_cuit": row["cuit_representado"].strip(),
                    "contrasena": row["contrasena"],
                    "descarga_emitidos": bool(descarga_emitidos),
                    "descarga_recibidos": bool(descarga_recibidos),
                    "proxy_request": bool(proxy_request),
                    # La API acepta nuevas opciones para determinar dónde cargar los archivos y si se desea la respuesta JSON.
                    "carga_s3": bool(carga_s3),
                    "carga_minio": bool(carga_minio),
                    "carga_json": bool(carga_json),
                    "b64": False
                }
                resp = call_consulta(base_url, headers, payload)
                http_status = resp.get("http_status")
                data = resp.get("data", {})

                success = data.get("success") if isinstance(data, dict) else None
                message = data.get("message") if isinstance(data, dict) else None
                header_obj = data.get("header") if isinstance(data, dict) else None
                error_obj = data.get("error") if isinstance(data, dict) else None

                urls = pick_url_fields(data)

                out_rows.append({
                    "cuit_inicio_sesion": row["cuit_inicio_sesion"],
                    "nombre_representado": row["nombre_representado"],
                    "cuit_representado": row["cuit_representado"],
                    "http_status": http_status,
                    "success": success,
                    "message": message,
                    "emitidos_url_s3": urls["mis_comprobantes_emitidos_url_s3"],
                    "emitidos_url_minio": urls["mis_comprobantes_emitidos_url_minio"],
                    "recibidos_url_s3": urls["mis_comprobantes_recibidos_url_s3"],
                    "recibidos_url_minio": urls["mis_comprobantes_recibidos_url_minio"],
                    "header_json": str(header_obj) if header_obj is not None else None,
                    "error_json": str(error_obj) if error_obj is not None else None,
                })

                progress.progress(int((idx + 1) / len(input_df) * 100))

            status_ph.success("Procesamiento finalizado.")
            result_df = pd.DataFrame(out_rows)

            ok_count = result_df["success"].fillna(False).sum()
            st.metric(label="Consultas exitosas", value=int(ok_count))
            st.metric(label="Consultas totales", value=len(result_df))

            st.markdown("### ✅ Consolidado de URLs (vista previa)")
            st.dataframe(result_df.head(50), use_container_width=True)

            xlsx_bytes = make_output_excel(result_df, sheet_name="Consolidado_URLs")
            st.download_button(
                label="⬇️ Descargar Excel Consolidado",
                data=xlsx_bytes,
                file_name=f"consolidado_mis_comprobantes_{date.today().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_consolidado_tab1"
            )

            st.caption("Nota: No se almacenan contraseñas ni datos del Excel en el servidor. El procesamiento ocurre en memoria.")

# -------------------------------------------------------------------
# TAB 2: Consultas disponibles
# -------------------------------------------------------------------
with tab2:
    st.subheader("2) Consultar cantidad de consultas disponibles")
    st.write("Consulta el endpoint **GET** `/api/v1/user/consultas/{email}`.")
    q_email = st.text_input("Email (path param)", value=header_email or "", help="Se usa como parte de la URL.")
    headers = build_headers(x_api_key, header_email)

    if st.button("🔎 Consultar", key="btn_consultas_disponibles"):
        if not q_email.strip():
            st.warning("Ingresá un email para consultar.")
        else:
            http_status, data_json, consultas_disponibles, err = call_consultas_disponibles(base_url, q_email.strip(), headers)
            if err:
                st.error(err)
            else:
                st.info(f"HTTP status: {http_status}")
                if data_json is not None:
                    st.json(data_json)
                    if consultas_disponibles is not None:
                        st.metric("Consultas disponibles", int(consultas_disponibles))

# -------------------------------------------------------------------
# TAB 3: Descarga de archivos S3/MinIO a ZIP
# -------------------------------------------------------------------
with tab3:
    st.subheader("3) Descargar columnas MinIO del consolidado → ZIP (Emitidos/Recibidos)")
    st.write(
        "Subí el **Excel consolidado** de la solapa 1. Se leerán preferentemente las columnas `emitidos_url_minio` y `recibidos_url_minio`. "
        "Si no existen, se intentará usar las columnas de S3. Los archivos de MinIO se descargan "
        "con su **nombre original** y **sin extraer** contenidos. "
        "Si existe `cuit_representado` (o `representado_cuit`), se crearán subcarpetas por contribuyente."
    )
    up_zip = st.file_uploader("Seleccionar consolidado (.xlsx)", type=["xlsx"], key="uploader_tab3")

    with st.expander("🔧 Opciones de descarga"):
        timeout_zip = st.number_input("Timeout por archivo (segundos)", min_value=10, value=120, step=10)

    if up_zip is not None:
        try:
            df_zip = pd.read_excel(up_zip, dtype=str).fillna("")
        except Exception as e:
            st.error(f"Error leyendo el Excel: {e}")
            st.stop()

        df_zip.columns = [c.strip().lower() for c in df_zip.columns]
        # Determinar las columnas disponibles. Preferir las URLs de MinIO y luego S3.
        col_emitidos_minio = "emitidos_url_minio" if "emitidos_url_minio" in df_zip.columns else None
        col_recibidos_minio = "recibidos_url_minio" if "recibidos_url_minio" in df_zip.columns else None
        col_emitidos_s3 = "emitidos_url_s3" if "emitidos_url_s3" in df_zip.columns else None
        col_recibidos_s3 = "recibidos_url_s3" if "recibidos_url_s3" in df_zip.columns else None
        # Validar existencia de al menos una columna para cada tipo.
        if not (col_emitidos_minio or col_emitidos_s3) or not (col_recibidos_minio or col_recibidos_s3):
            st.error(
                "El Excel no posee las columnas requeridas: 'emitidos_url_minio'/'emitidos_url_s3' y 'recibidos_url_minio'/'recibidos_url_s3'."
            )
            st.stop()

        contrib_col = None
        for cand in ("cuit_representado", "representado_cuit"):
            if cand in df_zip.columns:
                contrib_col = cand
                break

        # Extraer listas de URLs según la prioridad MinIO -> S3
        if col_emitidos_minio:
            urls_emitidos = collect_url_entries_from_df(
                df_zip,
                col_emitidos_minio,
                contrib_col,
                extract_zip=False
            )
        else:
            urls_emitidos = collect_url_entries_from_df(
                df_zip,
                col_emitidos_s3,
                contrib_col,
                extract_zip=True
            )
        if col_recibidos_minio:
            urls_recibidos = collect_url_entries_from_df(
                df_zip,
                col_recibidos_minio,
                contrib_col,
                extract_zip=False
            )
        else:
            urls_recibidos = collect_url_entries_from_df(
                df_zip,
                col_recibidos_s3,
                contrib_col,
                extract_zip=True
            )

        st.write(f"URLs en **Emitidos**: {len(urls_emitidos)} | URLs en **Recibidos**: {len(urls_recibidos)}")

        if st.button("📦 Generar ZIP con descargas", key="btn_zip"):
            with st.spinner("Descargando archivos y construyendo ZIP..."):
                zip_bytes, log_df = download_to_zip(
                    urls_emitidos=urls_emitidos,
                    urls_recibidos=urls_recibidos,
                    timeout_sec=int(timeout_zip)
                )

            st.download_button(
                label="⬇️ Descargar ZIP (Emitidos/Recibidos)",
                data=zip_bytes,
                # El nombre de archivo se mantiene genérico ya que puede contener descargas de MinIO o S3.
                file_name=f"descargas_{date.today().strftime('%Y%m%d')}.zip",
                mime="application/zip",
                key="download_zip_tab3"
            )

            # Log opcional en Excel
            log_xlsx = make_output_excel(log_df, sheet_name="LogDescargas")
            st.download_button(
                label="🗒️ Descargar Log (Excel)",
                data=log_xlsx,
                file_name=f"log_descargas_{date.today().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_log_tab3"
            )

    st.caption(
        "Nota: los links MinIO se guardan tal cual (sin extracción). "
        "Si se usan links S3, se mantienen las extracciones de ZIP internos."
    )

# -------------------------------------------------------------------
# TAB 4: Consolidar salidas (ZIP → 2 Excel) — SIN VISTA PREVIA
# -------------------------------------------------------------------
with tab4:
    st.subheader("4) Consolidar archivos de salida (ZIP → 2 Excel)")
    st.write(
        "Importá el **ZIP** con las carpetas `Emitidos/` y `Recibidos/` (CSV con separador `;`). "
        "Se generará un **ZIP** con dos archivos: **Consolidados Emitidos.xlsx** y **Consolidados Recibidos.xlsx**."
    )
    zip_in = st.file_uploader("Seleccioná el ZIP con `Emitidos/` y `Recibidos/`", type=["zip"], key="uploader_tab4")

    if zip_in is not None and st.button("🧩 Consolidar ZIP → 2 Excel (descargar ZIP)", key="btn_consolidar_zip"):
        try:
            with zipfile.ZipFile(zip_in) as zf:
                df_emitidos = consolidate_group_from_zip(zf, "Emitidos")
                df_recibidos = consolidate_group_from_zip(zf, "Recibidos")
        except zipfile.BadZipFile:
            st.error("El archivo subido no es un ZIP válido.")
            st.stop()
        except Exception as e:
            st.error(f"Error procesando el ZIP: {e}")
            st.stop()

        # Construir ZIP de salida con ambos Excel
        out_zip_bytes = build_zip_with_excels(df_emitidos, df_recibidos)
        st.download_button(
            label="⬇️ Descargar ZIP con Consolidados (Emitidos/Recibidos)",
            data=out_zip_bytes,
            file_name=f"Consolidados_{date.today().strftime('%Y%m%d')}.zip",
            mime="application/zip",
            key="download_consolidados_zip"
        )

# -------------------------------------------------------------------
# TAB 5: Otros endpoints (Comprobantes en Línea, SCT, CCMA, Apócrifos, Consulta de CUIT)
# -------------------------------------------------------------------
# Ajuste: cada endpoint ahora es su propia solapa principal.
with tab_rcel:
    st.subheader("RCEL")
    subtab_rcel = tab_rcel  # Reutilizar variable para el contenido existente
with tab_sct:
    st.subheader("SCT")
    subtab_sct = tab_sct
with tab_ccma:
    st.subheader("CCMA")
    subtab_ccma = tab_ccma
with tab_apoc:
    st.subheader("APOC")
    subtab_apoc = tab_apoc
with tab_cuit:
    st.subheader("Consulta de Constancia de CUIT")
    subtab_cuit = tab_cuit

    # -------------------------------------------------------------
    # Subtab: Comprobantes en Línea (RCEL)
    # -------------------------------------------------------------
    with subtab_rcel:
        st.markdown("### Comprobantes en Línea (RCEL)")
        st.write(
            "Consulta facturas emitidas en el servicio Comprobantes en Línea. "
            "Seleccioná el modo de consulta: individual o masivo. En modo masivo se utiliza un Excel con "
            "los datos de cada contribuyente."
        )
        rcel_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="rcel_mode")
        # Fechas son comunes a ambos modos
        rcel_desde = st.date_input("Desde", value=date(date.today().year, 1, 1), format="DD/MM/YYYY", key="rcel_desde_date")
        rcel_hasta = st.date_input("Hasta", value=date.today(), format="DD/MM/YYYY", key="rcel_hasta_date")
        # Opciones comunes de salida
        rcel_b64_pdf = st.checkbox("PDF en base64", value=False, help="Marcar para recibir el PDF codificado en base64", key="rcel_b64_pdf")
        rcel_minio = st.checkbox("Subir PDF a MinIO", value=True, help="Marcar para recibir un enlace de descarga desde MinIO", key="rcel_minio_option")
        if rcel_mode == "Individual":
            rc_cuit_rep = st.text_input("CUIT del representante", value="", key="rcel_cuit_rep_ind")
            rc_nombre = st.text_input("Nombre exacto del contribuyente (nombre_rcel)", value="", key="rcel_nombre_ind")
            rc_cuit_repr = st.text_input("CUIT del contribuyente representado", value="", key="rcel_cuit_repr_ind")
            rc_clave = st.text_input("Clave fiscal", value="", type="password", key="rcel_clave_ind")
            if st.button("Consultar RCEL", key="btn_rcel_consulta_ind"):
                if not (rc_cuit_rep.strip() and rc_nombre.strip() and rc_cuit_repr.strip() and rc_clave.strip()):
                    st.warning("Completá todos los campos obligatorios (CUIT representante, nombre, CUIT representado y clave fiscal).")
                else:
                    headers_local = build_headers(x_api_key, header_email)
                    payload_rcel = {
                        "desde": as_ddmmyyyy(rcel_desde),
                        "hasta": as_ddmmyyyy(rcel_hasta),
                        "cuit_representante": rc_cuit_rep.strip(),
                        "nombre_rcel": rc_nombre.strip(),
                        "representado_cuit": rc_cuit_repr.strip(),
                        "clave": rc_clave,
                        "b64_pdf": bool(rcel_b64_pdf),
                        "minio_upload": bool(rcel_minio)
                    }
                    with st.spinner("Consultando RCEL..."):
                        resp_rcel = call_rcel_consulta(base_url, headers_local, payload_rcel)
                    st.info(f"HTTP status: {resp_rcel.get('http_status')}")
                    st.json(resp_rcel.get("data"))
                    st.session_state["rcel_last_response"] = resp_rcel.get("data")
                    st.session_state["rcel_last_cuit_repr"] = rc_cuit_repr.strip()

            last_rcel_data = st.session_state.get("rcel_last_response")
            last_rcel_cuit = st.session_state.get("rcel_last_cuit_repr", "").strip()
            if last_rcel_data is not None and last_rcel_cuit:
                # Botón para descargar ZIP con archivos de MinIO
                if st.button("📦 Generar ZIP con archivos MinIO", key="btn_rcel_zip_ind"):
                    with st.spinner("Descargando archivos desde MinIO..."):
                        zip_bytes, log_df = download_minio_to_zip_by_contributor(
                            [{"representado_cuit": last_rcel_cuit, "data": json.dumps(last_rcel_data, ensure_ascii=False)}],
                            url_field="data",
                            contributor_field="representado_cuit"
                        )
                    if len(log_df) > 0:
                        st.success(f"ZIP generado: {len(log_df)} operaciones")
                        col1, col2 = st.columns(2)
                        with col1:
                            st.download_button(
                                label="⬇️ Descargar ZIP",
                                data=zip_bytes,
                                file_name=f"rcel_{last_rcel_cuit}_{date.today().strftime('%Y%m%d')}.zip",
                                mime="application/zip",
                                key="download_rcel_zip_ind"
                            )
                        with col2:
                            log_xlsx = make_output_excel(log_df, sheet_name="Log")
                            st.download_button(
                                label="📋 Log",
                                data=log_xlsx,
                                file_name=f"log_rcel_{last_rcel_cuit}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_rcel_log_ind"
                            )
                    else:
                        st.info("No se encontraron URLs de MinIO en la respuesta.")
        else:
            render_minio_mass_download(
                section_title="RCEL",
                uploader_key="rcel_minio_file_upload",
                timeout_key="rcel_minio_timeout",
                button_key="btn_rcel_minio_zip"
            )

    # -------------------------------------------------------------
    # Subtab: Sistema de Cuentas Tributarias (SCT)
    # -------------------------------------------------------------
    with subtab_sct:
        st.markdown("### Sistema de Cuentas Tributarias (SCT)")
        st.write(
            "Consulta el estado del Sistema de Cuentas Tributarias. Elige modo individual o masivo. "
            "La respuesta puede incluir archivos (Excel, CSV o PDF) codificados en base64 o enlaces a MinIO."
        )
        sct_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="sct_mode")
        # Opciones de formatos son comunes a ambos modos
        st.caption("Seleccioná en qué formatos querés recibir los archivos.")
        col_sct1, col_sct2, col_sct3 = st.columns(3)
        with col_sct1:
            sct_excel_minio = st.checkbox("Excel (MinIO)", value=True, key="sct_excel_minio_option")
            sct_excel_b64 = st.checkbox("Excel (Base64)", value=False, key="sct_excel_b64_option")
        with col_sct2:
            sct_csv_minio = st.checkbox("CSV (MinIO)", value=False, key="sct_csv_minio_option")
            sct_csv_b64 = st.checkbox("CSV (Base64)", value=False, key="sct_csv_b64_option")
        with col_sct3:
            sct_pdf_minio = st.checkbox("PDF (MinIO)", value=False, key="sct_pdf_minio_option")
            sct_pdf_b64 = st.checkbox("PDF (Base64)", value=False, key="sct_pdf_b64_option")
        sct_proxy = st.checkbox("Usar proxy_request", value=False, key="sct_proxy_option")
        if sct_mode == "Individual":
            sct_cuit_login = st.text_input("CUIT login", value="", key="sct_cuit_login_ind")
            sct_clave = st.text_input("Clave fiscal", value="", type="password", key="sct_clave_ind")
            sct_cuit_repr = st.text_input("CUIT representado", value="", key="sct_cuit_repr_ind")
            if st.button("Consultar SCT", key="btn_sct_consulta_ind"):
                if not (sct_cuit_login.strip() and sct_clave.strip() and sct_cuit_repr.strip()):
                    st.warning("Completá todos los campos obligatorios (CUIT login, clave y CUIT representado).")
                else:
                    headers_local = build_headers(x_api_key, header_email)
                    payload_sct = {
                        "cuit_login": sct_cuit_login.strip(),
                        "clave": sct_clave,
                        "cuit_representado": sct_cuit_repr.strip(),
                        "excel_b64": bool(sct_excel_b64),
                        "csv_b64": bool(sct_csv_b64),
                        "pdf_b64": bool(sct_pdf_b64),
                        "excel_minio": bool(sct_excel_minio),
                        "csv_minio": bool(sct_csv_minio),
                        "pdf_minio": bool(sct_pdf_minio),
                        "proxy_request": bool(sct_proxy)
                    }
                    with st.spinner("Consultando SCT..."):
                        resp_sct = call_sct_consulta(base_url, headers_local, payload_sct)
                    st.info(f"HTTP status: {resp_sct.get('http_status')}")
                    data_sct = resp_sct.get("data")
                    st.json(data_sct)
                    if isinstance(data_sct, dict):
                        for key in ["excel_url_minio", "csv_url_minio", "pdf_url_minio"]:
                            url_val = data_sct.get(key)
                            if url_val:
                                st.markdown(f"[{key}]({url_val})")
                        for key_b64, ext, mime in [
                            ("excel_b64", ".xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                            ("csv_b64", ".csv", "text/csv"),
                            ("pdf_b64", ".pdf", "application/pdf"),
                        ]:
                            b64_val = data_sct.get(key_b64)
                            if b64_val:
                                try:
                                    file_bytes = base64.b64decode(b64_val)
                                    file_name = f"sct_{key_b64}{ext}"
                                    st.download_button(
                                        label=f"⬇️ Descargar {key_b64}",
                                        data=file_bytes,
                                        file_name=file_name,
                                        mime=mime,
                                        key=f"download_{key_b64}_sct_ind"
                                    )
                                except Exception:
                                    pass
                    st.session_state["sct_last_response"] = data_sct
                    st.session_state["sct_last_cuit_repr"] = sct_cuit_repr.strip()

            last_sct_data = st.session_state.get("sct_last_response")
            last_sct_cuit = st.session_state.get("sct_last_cuit_repr", "").strip()
            if last_sct_data is not None and last_sct_cuit:
                # Botón para descargar ZIP con archivos de MinIO
                if st.button("📦 Generar ZIP con archivos MinIO", key="btn_sct_zip_ind"):
                    with st.spinner("Descargando archivos desde MinIO..."):
                        zip_bytes, log_df = download_minio_to_zip_by_contributor(
                            [{"cuit_representado": last_sct_cuit, "data": json.dumps(last_sct_data, ensure_ascii=False)}],
                            url_field="data",
                            contributor_field="cuit_representado"
                        )
                    if len(log_df) > 0:
                        st.success(f"ZIP generado: {len(log_df)} operaciones")
                        col1, col2 = st.columns(2)
                        with col1:
                            st.download_button(
                                label="⬇️ Descargar ZIP",
                                data=zip_bytes,
                                file_name=f"sct_{last_sct_cuit}_{date.today().strftime('%Y%m%d')}.zip",
                                mime="application/zip",
                                key="download_sct_zip_ind"
                            )
                        with col2:
                            log_xlsx = make_output_excel(log_df, sheet_name="Log")
                            st.download_button(
                                label="📋 Log",
                                data=log_xlsx,
                                file_name=f"log_sct_{last_sct_cuit}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_sct_log_ind"
                            )
                    else:
                        st.info("No se encontraron URLs de MinIO en la respuesta.")
        else:
            render_minio_mass_download(
                section_title="SCT",
                uploader_key="sct_minio_file_upload",
                timeout_key="sct_minio_timeout",
                button_key="btn_sct_minio_zip"
            )

    # -------------------------------------------------------------
    # Subtab: CCMA (Cuenta Corriente de Monotributistas y Autónomos)
    # -------------------------------------------------------------
    with subtab_ccma:
        st.markdown("### Cuenta Corriente de Monotributistas y Autónomos (CCMA)")
        st.write(
            "Consulta la cuenta corriente de uno o varios contribuyentes y descargá los movimientos con montos formateados "
            "(debe/haber, saldos) listos para Excel."
        )
        ccma_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="ccma_mode")
        col_ccma_flags = st.columns(2)
        with col_ccma_flags[0]:
            ccma_proxy = st.checkbox("Usar proxy_request", value=False, key="ccma_proxy_option")
        with col_ccma_flags[1]:
            ccma_movimientos = st.checkbox(
                "Solicitar movimientos", value=True, key="ccma_movimientos_option",
                help="Incluye movimientos y saldos formateados en la salida."
            )
        if ccma_mode == "Individual":
            ccma_cuit_rep = st.text_input("CUIT del representante", value="", key="ccma_cuit_rep_ind")
            ccma_clave_rep = st.text_input("Clave fiscal del representante", value="", type="password", key="ccma_clave_rep_ind")
            ccma_cuit_repr = st.text_input("CUIT del representado", value="", key="ccma_cuit_repr_ind")
            if st.button("Consultar CCMA", key="btn_ccma_consulta_ind"):
                if not (ccma_cuit_rep.strip() and ccma_clave_rep.strip() and ccma_cuit_repr.strip()):
                    st.warning("Completá todos los campos obligatorios (CUIT representante, clave y CUIT representado).")
                else:
                    headers_local = build_headers(x_api_key, header_email)
                    payload_ccma = {
                        "cuit_representante": ccma_cuit_rep.strip(),
                        "clave_representante": ccma_clave_rep,
                        "cuit_representado": ccma_cuit_repr.strip(),
                        "proxy_request": bool(ccma_proxy),
                        "movimientos": bool(ccma_movimientos)
                    }
                    with st.spinner("Consultando CCMA..."):
                        resp_ccma = call_ccma_consulta(base_url, headers_local, payload_ccma)
                    st.info(f"HTTP status: {resp_ccma.get('http_status')}")
                    data_ccma = resp_ccma.get("data")
                    st.json(data_ccma)
                    resumen_row, movimientos_rows = normalize_ccma_response(
                        resp_ccma.get("http_status"),
                        data_ccma,
                        ccma_cuit_rep.strip(),
                        ccma_cuit_repr.strip(),
                        ccma_movimientos
                    )
                    resumen_df, movimientos_df = build_ccma_outputs([resumen_row], movimientos_rows, ccma_movimientos)
                    st.write("### Resultado formateado CCMA (vista previa)")
                    st.dataframe(resumen_df, use_container_width=True)
                    if ccma_movimientos or not movimientos_df.empty:
                        st.write("### Movimientos formateados (vista previa)")
                        st.dataframe(movimientos_df.head(50), use_container_width=True)
                    excel_ccma = build_ccma_excel(resumen_df, movimientos_df, include_movements_sheet=ccma_movimientos)
                    st.download_button(
                        label="⬇️ Descargar Excel CCMA (resumen y movimientos)",
                        data=excel_ccma,
                        file_name=f"ccma_{date.today().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="download_ccma_ind"
                    )
                    st.session_state["ccma_last_response_json"] = resumen_row.get("response_json", "{}")
                    st.session_state["ccma_last_cuit_repr"] = ccma_cuit_repr.strip()

            last_ccma_json = st.session_state.get("ccma_last_response_json")
            last_ccma_cuit = st.session_state.get("ccma_last_cuit_repr", "").strip()
            if last_ccma_json is not None and last_ccma_cuit:
                # Botón para descargar ZIP con archivos de MinIO
                if st.button("📦 Generar ZIP con archivos MinIO", key="btn_ccma_zip_ind"):
                    with st.spinner("Descargando archivos desde MinIO..."):
                        zip_bytes, log_df = download_minio_to_zip_by_contributor(
                            [{"cuit_representado": last_ccma_cuit, "response_json": last_ccma_json}],
                            url_field="response_json",
                            contributor_field="cuit_representado"
                        )
                    if len(log_df) > 0:
                        st.success(f"ZIP generado: {len(log_df)} operaciones")
                        col1, col2 = st.columns(2)
                        with col1:
                            st.download_button(
                                label="⬇️ Descargar ZIP",
                                data=zip_bytes,
                                file_name=f"ccma_{last_ccma_cuit}_{date.today().strftime('%Y%m%d')}.zip",
                                mime="application/zip",
                                key="download_ccma_zip_ind"
                            )
                        with col2:
                            log_xlsx = make_output_excel(log_df, sheet_name="Log")
                            st.download_button(
                                label="📋 Log",
                                data=log_xlsx,
                                file_name=f"log_ccma_{last_ccma_cuit}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_ccma_log_ind"
                            )
                    else:
                        st.info("No se encontraron URLs de MinIO en la respuesta.")
        else:
            render_minio_mass_download(
                section_title="CCMA",
                uploader_key="ccma_minio_file_upload",
                timeout_key="ccma_minio_timeout",
                button_key="btn_ccma_minio_zip"
            )

    # -------------------------------------------------------------
    # TAB: Mis Retenciones
    # -------------------------------------------------------------
    with tab_mis_retenciones:
        st.markdown("### Mis Retenciones")
        st.write(
            "Consulta retenciones de AFIP. Selecciona modo individual o masivo (con Excel). "
            "Soporta descarga automática desde MinIO."
        )
        mr_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="mr_mode", horizontal=True)
        mr_desde = st.date_input("Desde", value=date(date.today().year, 1, 1), format="DD/MM/YYYY", key="mr_desde_date")
        mr_hasta = st.date_input("Hasta", value=date.today(), format="DD/MM/YYYY", key="mr_hasta_date")
        mr_minio = st.checkbox("Carga a MinIO", value=True, key="mr_minio_option")
        mr_proxy = st.checkbox("Usar proxy_request", value=False, key="mr_proxy_option")
        
        if mr_mode == "Individual":
            mr_cuit_rep = st.text_input("CUIT representante", value="", key="mr_cuit_rep_ind")
            mr_clave = st.text_input("Clave representante", value="", type="password", key="mr_clave_ind")
            mr_cuit_repr = st.text_input("CUIT representado (opcional)", value="", key="mr_cuit_repr_ind")
            mr_denominacion = st.text_input("Denominación", value="", key="mr_denominacion_ind")
            
            if st.button("Consultar Mis Retenciones", key="btn_mr_consulta_ind"):
                if not (mr_cuit_rep.strip() and mr_clave.strip() and mr_denominacion.strip()):
                    st.warning("Completá CUIT representante, clave y denominación.")
                else:
                    headers_local = build_headers(x_api_key, header_email)
                    payload_mr = {
                        "cuit_representante": mr_cuit_rep.strip(),
                        "clave_representante": mr_clave,
                        "cuit_representado": mr_cuit_repr.strip() if mr_cuit_repr.strip() else None,
                        "denominacion": mr_denominacion.strip(),
                        "desde": as_ddmmyyyy(mr_desde),
                        "hasta": as_ddmmyyyy(mr_hasta),
                        "carga_minio": bool(mr_minio),
                        "proxy_request": bool(mr_proxy)
                    }
                    with st.spinner("Consultando Mis Retenciones..."):
                        resp_mr = call_mis_retenciones_consulta(base_url, headers_local, payload_mr)
                    st.info(f"HTTP status: {resp_mr.get('http_status')}")
                    st.json(resp_mr.get("data"))
                    cuit_id = mr_cuit_repr.strip() if mr_cuit_repr.strip() else mr_cuit_rep.strip()
                    st.session_state["mr_last_response"] = resp_mr.get("data")
                    st.session_state["mr_last_cuit_id"] = cuit_id

            last_mr_data = st.session_state.get("mr_last_response")
            last_mr_cuit = st.session_state.get("mr_last_cuit_id", "").strip()
            if last_mr_data is not None and last_mr_cuit:
                # Botón para descargar ZIP con archivos de MinIO
                if st.button("📦 Generar ZIP con archivos MinIO", key="btn_mr_zip_ind"):
                    with st.spinner("Descargando archivos desde MinIO..."):
                        zip_bytes, log_df = download_minio_to_zip_by_contributor(
                            [{"cuit_representado": last_mr_cuit, "data": json.dumps(last_mr_data, ensure_ascii=False)}],
                            url_field="data",
                            contributor_field="cuit_representado"
                        )
                    if len(log_df) > 0:
                        st.success(f"ZIP generado: {len(log_df)} operaciones")
                        col1, col2 = st.columns(2)
                        with col1:
                            st.download_button(
                                label="⬇️ Descargar ZIP",
                                data=zip_bytes,
                                file_name=f"mis_retenciones_{last_mr_cuit}_{date.today().strftime('%Y%m%d')}.zip",
                                mime="application/zip",
                                key="download_mr_zip_ind"
                            )
                        with col2:
                            log_xlsx = make_output_excel(log_df, sheet_name="Log")
                            st.download_button(
                                label="📋 Log",
                                data=log_xlsx,
                                file_name=f"log_mis_retenciones_{last_mr_cuit}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_mr_log_ind"
                            )
                    else:
                        st.info("No se encontraron URLs de MinIO en la respuesta.")
        else:
            render_minio_mass_download(
                section_title="Mis_Retenciones",
                uploader_key="mr_minio_file_upload",
                timeout_key="mr_minio_timeout",
                button_key="btn_mr_minio_zip"
            )

    # -------------------------------------------------------------
    # TAB: SIFERE
    # -------------------------------------------------------------
    with tab_sifere:
        st.markdown("### SIFERE - Sistema Federal de Recaudación")
        st.write("Consulta SIFERE por jurisdicción. Modo individual o masivo (Excel).")
        sifere_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="sifere_mode", horizontal=True)
        sifere_periodo = st.text_input("Período (ej: 202401)", value="", key="sifere_periodo")
        sifere_minio = st.checkbox("Carga a MinIO", value=True, key="sifere_minio_option")
        sifere_proxy = st.checkbox("Usar proxy_request", value=False, key="sifere_proxy_option")
        
        if sifere_mode == "Individual":
            sifere_cuit_rep = st.text_input("CUIT representante", value="", key="sifere_cuit_rep_ind")
            sifere_clave = st.text_input("Clave representante", value="", type="password", key="sifere_clave_ind")
            sifere_cuit_repr = st.text_input("CUIT representado", value="", key="sifere_cuit_repr_ind")
            sifere_nombre = st.text_input("Nombre representado (opcional)", value="", key="sifere_nombre_ind")
            sifere_jurisdicciones = st.text_input("Jurisdicciones (ej: 901,902)", value="", key="sifere_jurisdicciones_ind")
            
            if st.button("Consultar SIFERE", key="btn_sifere_consulta_ind"):
                if not (sifere_cuit_rep.strip() and sifere_clave.strip() and sifere_cuit_repr.strip() and sifere_periodo.strip()):
                    st.warning("Completá CUIT representante, clave, CUIT representado y período.")
                else:
                    jurisdicciones_list = [j.strip() for j in sifere_jurisdicciones.split(",") if j.strip()]
                    headers_local = build_headers(x_api_key, header_email)
                    payload_sifere = {
                        "cuit_representante": sifere_cuit_rep.strip(),
                        "clave_representante": sifere_clave,
                        "cuit_representado": sifere_cuit_repr.strip(),
                        "periodo": sifere_periodo.strip(),
                        "representado_nombre": sifere_nombre.strip() if sifere_nombre.strip() else None,
                        "jurisdicciones": jurisdicciones_list,
                        "carga_minio": bool(sifere_minio),
                        "proxy_request": bool(sifere_proxy)
                    }
                    with st.spinner("Consultando SIFERE..."):
                        resp_sifere = call_sifere_consulta(base_url, headers_local, payload_sifere)
                    st.info(f"HTTP status: {resp_sifere.get('http_status')}")
                    st.json(resp_sifere.get("data"))
                    st.session_state["sifere_last_response"] = resp_sifere.get("data")
                    st.session_state["sifere_last_cuit_repr"] = sifere_cuit_repr.strip()

            last_sifere_data = st.session_state.get("sifere_last_response")
            last_sifere_cuit = st.session_state.get("sifere_last_cuit_repr", "").strip()
            if last_sifere_data is not None and last_sifere_cuit:
                # Botón para descargar ZIP con archivos de MinIO
                if st.button("📦 Generar ZIP con archivos MinIO", key="btn_sifere_zip_ind"):
                    with st.spinner("Descargando archivos desde MinIO..."):
                        zip_bytes, log_df = download_minio_to_zip_by_contributor(
                            [{"cuit_representado": last_sifere_cuit, "data": json.dumps(last_sifere_data, ensure_ascii=False)}],
                            url_field="data",
                            contributor_field="cuit_representado"
                        )
                    if len(log_df) > 0:
                        st.success(f"ZIP generado: {len(log_df)} operaciones")
                        col1, col2 = st.columns(2)
                        with col1:
                            st.download_button(
                                label="⬇️ Descargar ZIP",
                                data=zip_bytes,
                                file_name=f"sifere_{last_sifere_cuit}_{date.today().strftime('%Y%m%d')}.zip",
                                mime="application/zip",
                                key="download_sifere_zip_ind"
                            )
                        with col2:
                            log_xlsx = make_output_excel(log_df, sheet_name="Log")
                            st.download_button(
                                label="📋 Log",
                                data=log_xlsx,
                                file_name=f"log_sifere_{last_sifere_cuit}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_sifere_log_ind"
                            )
                    else:
                        st.info("No se encontraron URLs de MinIO en la respuesta.")
        else:
            render_minio_mass_download(
                section_title="SIFERE",
                uploader_key="sifere_minio_file_upload",
                timeout_key="sifere_minio_timeout",
                button_key="btn_sifere_minio_zip"
            )

    # -------------------------------------------------------------
    # TAB: Declaración en Línea
    # -------------------------------------------------------------
    with tab_declaracion_linea:
        st.markdown("### Declaración en Línea")
        st.write("Consulta declaraciones juradas presentadas. Modo individual o masivo (Excel).")
        decl_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="decl_mode", horizontal=True)
        decl_periodo_desde = st.text_input("Período desde (ej: 202401)", value="", key="decl_periodo_desde")
        decl_periodo_hasta = st.text_input("Período hasta (ej: 202412)", value="", key="decl_periodo_hasta")
        decl_minio = st.checkbox("Carga a MinIO", value=True, key="decl_minio_option")
        decl_proxy = st.checkbox("Usar proxy_request", value=False, key="decl_proxy_option")
        
        if decl_mode == "Individual":
            decl_cuit_rep = st.text_input("CUIT representante", value="", key="decl_cuit_rep_ind")
            decl_clave = st.text_input("Clave representante", value="", type="password", key="decl_clave_ind")
            decl_cuit_repr = st.text_input("CUIT representado (opcional)", value="", key="decl_cuit_repr_ind")
            decl_nombre = st.text_input("Nombre representado (opcional)", value="", key="decl_nombre_ind")
            
            if st.button("Consultar Declaración en Línea", key="btn_decl_consulta_ind"):
                if not (decl_cuit_rep.strip() and decl_clave.strip() and decl_periodo_desde.strip() and decl_periodo_hasta.strip()):
                    st.warning("Completá CUIT representante, clave y períodos.")
                else:
                    headers_local = build_headers(x_api_key, header_email)
                    payload_decl = {
                        "cuit_representante": decl_cuit_rep.strip(),
                        "clave_representante": decl_clave,
                        "cuit_representado": decl_cuit_repr.strip() if decl_cuit_repr.strip() else None,
                        "representado_nombre": decl_nombre.strip() if decl_nombre.strip() else None,
                        "periodo_desde": decl_periodo_desde.strip(),
                        "periodo_hasta": decl_periodo_hasta.strip(),
                        "carga_minio": bool(decl_minio),
                        "proxy_request": bool(decl_proxy)
                    }
                    with st.spinner("Consultando Declaración en Línea..."):
                        resp_decl = call_declaracion_en_linea_consulta(base_url, headers_local, payload_decl)
                    st.info(f"HTTP status: {resp_decl.get('http_status')}")
                    st.json(resp_decl.get("data"))
                    cuit_id = decl_cuit_repr.strip() if decl_cuit_repr.strip() else decl_cuit_rep.strip()
                    st.session_state["decl_last_response"] = resp_decl.get("data")
                    st.session_state["decl_last_cuit_id"] = cuit_id

            last_decl_data = st.session_state.get("decl_last_response")
            last_decl_cuit = st.session_state.get("decl_last_cuit_id", "").strip()
            if last_decl_data is not None and last_decl_cuit:
                # Botón para descargar ZIP con archivos de MinIO
                if st.button("📦 Generar ZIP con archivos MinIO", key="btn_decl_zip_ind"):
                    with st.spinner("Descargando archivos desde MinIO..."):
                        zip_bytes, log_df = download_minio_to_zip_by_contributor(
                            [{"cuit_representado": last_decl_cuit, "data": json.dumps(last_decl_data, ensure_ascii=False)}],
                            url_field="data",
                            contributor_field="cuit_representado"
                        )
                    if len(log_df) > 0:
                        st.success(f"ZIP generado: {len(log_df)} operaciones")
                        col1, col2 = st.columns(2)
                        with col1:
                            st.download_button(
                                label="⬇️ Descargar ZIP",
                                data=zip_bytes,
                                file_name=f"declaracion_linea_{last_decl_cuit}_{date.today().strftime('%Y%m%d')}.zip",
                                mime="application/zip",
                                key="download_decl_zip_ind"
                            )
                        with col2:
                            log_xlsx = make_output_excel(log_df, sheet_name="Log")
                            st.download_button(
                                label="📋 Log",
                                data=log_xlsx,
                                file_name=f"log_declaracion_linea_{last_decl_cuit}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_decl_log_ind"
                            )
                    else:
                        st.info("No se encontraron URLs de MinIO en la respuesta.")
        else:
            st.markdown("#### Consulta masiva Declaración en Línea")
            st.write("Subí un Excel con: **cuit_representante**, **clave_representante**, **cuit_representado** (opcional), **representado_nombre** (opcional)")
            decl_file = st.file_uploader("Archivo Excel", type=["xlsx"], key="decl_file_upload")
            if decl_file:
                try:
                    df_decl = pd.read_excel(decl_file, dtype=str).fillna("")
                except Exception as e:
                    st.error(f"Error leyendo Excel: {e}")
                    df_decl = pd.DataFrame()
                df_decl.columns = [c.strip().lower() for c in df_decl.columns]
                required = ["cuit_representante", "clave_representante"]
                missing = [c for c in required if c not in df_decl.columns]
                if missing:
                    st.error(f"Faltan columnas: {', '.join(missing)}")
                else:
                    st.success(f"Filas leídas: {len(df_decl)}")
                    with st.expander("👀 Vista previa"):
                        st.dataframe(df_decl.head(10), use_container_width=True)
                    if st.button("Procesar Declaración en Línea masivo", key="btn_decl_masivo"):
                        headers_local = build_headers(x_api_key, header_email)
                        out_rows = []
                        progress = st.progress(0)
                        status_ph = st.empty()
                        for idx, row in df_decl.reset_index(drop=True).iterrows():
                            cuit_repr = row.get("cuit_representado", "").strip()
                            nombre = row.get("representado_nombre", "").strip()
                            status_ph.info(f"Procesando {idx+1}/{len(df_decl)} — {row['cuit_representante']}")
                            payload = {
                                "cuit_representante": row["cuit_representante"].strip(),
                                "clave_representante": row["clave_representante"],
                                "cuit_representado": cuit_repr if cuit_repr else None,
                                "representado_nombre": nombre if nombre else None,
                                "periodo_desde": decl_periodo_desde.strip(),
                                "periodo_hasta": decl_periodo_hasta.strip(),
                                "carga_minio": bool(decl_minio),
                                "proxy_request": bool(decl_proxy)
                            }
                            resp = call_declaracion_en_linea_consulta(base_url, headers_local, payload)
                            out_rows.append({
                                "cuit_representante": row["cuit_representante"],
                                "cuit_representado": cuit_repr,
                                "http_status": resp.get("http_status"),
                                "data": json.dumps(resp.get("data"), ensure_ascii=False)
                            })
                            progress.progress(int((idx + 1) / len(df_decl) * 100))
                        status_ph.success("Procesamiento finalizado.")
                        result_decl = pd.DataFrame(out_rows)
                        st.dataframe(result_decl.head(50), use_container_width=True)
                        xlsx_decl = make_output_excel(result_decl, sheet_name="Declaracion_Linea")
                        
                        col_dl1, col_dl2 = st.columns(2)
                        with col_dl1:
                            st.download_button(
                                label="⬇️ Descargar Excel Declaración en Línea",
                                data=xlsx_decl,
                                file_name=f"consolidado_declaracion_linea_{date.today().strftime('%Y%m%d')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_decl_masivo"
                            )
                        with col_dl2:
                            if st.button("📦 Generar ZIP con archivos MinIO", key="btn_decl_zip"):
                                with st.spinner("Descargando archivos desde MinIO..."):
                                    zip_bytes, log_df = download_minio_to_zip_by_contributor(
                                        out_rows,
                                        url_field="data",
                                        contributor_field="cuit_representado"
                                    )
                                st.success(f"ZIP generado: {len(log_df)} operaciones")
                                st.download_button(
                                    label="⬇️ Descargar ZIP de archivos",
                                    data=zip_bytes,
                                    file_name=f"declaracion_linea_archivos_{date.today().strftime('%Y%m%d')}.zip",
                                    mime="application/zip",
                                    key="download_decl_zip_files"
                                )
                                log_xlsx = make_output_excel(log_df, sheet_name="Log_Descargas")
                                st.download_button(
                                    label="📋 Descargar Log",
                                    data=log_xlsx,
                                    file_name=f"log_declaracion_linea_{date.today().strftime('%Y%m%d')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_decl_log"
                                )

    # -------------------------------------------------------------
    # TAB: Mis Facilidades
    # -------------------------------------------------------------
    with tab_mis_facilidades:
        st.markdown("### Mis Facilidades")
        st.write("Consulta planes de facilidades de pago. Modo individual o masivo (Excel).")
        fac_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="fac_mode", horizontal=True)
        fac_minio = st.checkbox("Carga a MinIO", value=True, key="fac_minio_option")
        fac_proxy = st.checkbox("Usar proxy_request", value=False, key="fac_proxy_option")
        
        if fac_mode == "Individual":
            fac_cuit_login = st.text_input("CUIT login", value="", key="fac_cuit_login_ind")
            fac_clave = st.text_input("Clave", value="", type="password", key="fac_clave_ind")
            fac_cuit_repr = st.text_input("CUIT representado (opcional)", value="", key="fac_cuit_repr_ind")
            fac_denominacion = st.text_input("Denominación (opcional)", value="", key="fac_denominacion_ind")
            
            if st.button("Consultar Mis Facilidades", key="btn_fac_consulta_ind"):
                if not (fac_cuit_login.strip() and fac_clave.strip()):
                    st.warning("Completá CUIT login y clave.")
                else:
                    headers_local = build_headers(x_api_key, header_email)
                    payload_fac = {
                        "cuit_login": fac_cuit_login.strip(),
                        "clave": fac_clave,
                        "cuit_representado": fac_cuit_repr.strip() if fac_cuit_repr.strip() else None,
                        "denominacion": fac_denominacion.strip() if fac_denominacion.strip() else None,
                        "carga_minio": bool(fac_minio),
                        "proxy_request": bool(fac_proxy)
                    }
                    with st.spinner("Consultando Mis Facilidades..."):
                        resp_fac = call_mis_facilidades_consulta(base_url, headers_local, payload_fac)
                    st.info(f"HTTP status: {resp_fac.get('http_status')}")
                    st.json(resp_fac.get("data"))
                    cuit_id = fac_cuit_repr.strip() if fac_cuit_repr.strip() else fac_cuit_login.strip()
                    st.session_state["fac_last_response"] = resp_fac.get("data")
                    st.session_state["fac_last_cuit_id"] = cuit_id

            last_fac_data = st.session_state.get("fac_last_response")
            last_fac_cuit = st.session_state.get("fac_last_cuit_id", "").strip()
            if last_fac_data is not None and last_fac_cuit:
                # Botón para descargar ZIP con archivos de MinIO
                if st.button("📦 Generar ZIP con archivos MinIO", key="btn_fac_zip_ind"):
                    with st.spinner("Descargando archivos desde MinIO..."):
                        zip_bytes, log_df = download_minio_to_zip_by_contributor(
                            [{"cuit_representado": last_fac_cuit, "data": json.dumps(last_fac_data, ensure_ascii=False)}],
                            url_field="data",
                            contributor_field="cuit_representado"
                        )
                    if len(log_df) > 0:
                        st.success(f"ZIP generado: {len(log_df)} operaciones")
                        col1, col2 = st.columns(2)
                        with col1:
                            st.download_button(
                                label="⬇️ Descargar ZIP",
                                data=zip_bytes,
                                file_name=f"mis_facilidades_{last_fac_cuit}_{date.today().strftime('%Y%m%d')}.zip",
                                mime="application/zip",
                                key="download_fac_zip_ind"
                            )
                        with col2:
                            log_xlsx = make_output_excel(log_df, sheet_name="Log")
                            st.download_button(
                                label="📋 Log",
                                data=log_xlsx,
                                file_name=f"log_mis_facilidades_{last_fac_cuit}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_fac_log_ind"
                            )
                    else:
                        st.info("No se encontraron URLs de MinIO en la respuesta.")
        else:
            st.markdown("#### Consulta masiva Mis Facilidades")
            st.write("Subí un Excel con: **cuit_login**, **clave**, **cuit_representado** (opcional), **denominacion** (opcional)")
            fac_file = st.file_uploader("Archivo Excel", type=["xlsx"], key="fac_file_upload")
            if fac_file:
                try:
                    df_fac = pd.read_excel(fac_file, dtype=str).fillna("")
                except Exception as e:
                    st.error(f"Error leyendo Excel: {e}")
                    df_fac = pd.DataFrame()
                df_fac.columns = [c.strip().lower() for c in df_fac.columns]
                required = ["cuit_login", "clave"]
                missing = [c for c in required if c not in df_fac.columns]
                if missing:
                    st.error(f"Faltan columnas: {', '.join(missing)}")
                else:
                    st.success(f"Filas leídas: {len(df_fac)}")
                    with st.expander("👀 Vista previa"):
                        st.dataframe(df_fac.head(10), use_container_width=True)
                    if st.button("Procesar Mis Facilidades masivo", key="btn_fac_masivo"):
                        headers_local = build_headers(x_api_key, header_email)
                        out_rows = []
                        progress = st.progress(0)
                        status_ph = st.empty()
                        for idx, row in df_fac.reset_index(drop=True).iterrows():
                            cuit_repr = row.get("cuit_representado", "").strip()
                            denom = row.get("denominacion", "").strip()
                            status_ph.info(f"Procesando {idx+1}/{len(df_fac)} — {row['cuit_login']}")
                            payload = {
                                "cuit_login": row["cuit_login"].strip(),
                                "clave": row["clave"],
                                "cuit_representado": cuit_repr if cuit_repr else None,
                                "denominacion": denom if denom else None,
                                "carga_minio": bool(fac_minio),
                                "proxy_request": bool(fac_proxy)
                            }
                            resp = call_mis_facilidades_consulta(base_url, headers_local, payload)
                            out_rows.append({
                                "cuit_login": row["cuit_login"],
                                "cuit_representado": cuit_repr,
                                "http_status": resp.get("http_status"),
                                "data": json.dumps(resp.get("data"), ensure_ascii=False)
                            })
                            progress.progress(int((idx + 1) / len(df_fac) * 100))
                        status_ph.success("Procesamiento finalizado.")
                        result_fac = pd.DataFrame(out_rows)
                        st.dataframe(result_fac.head(50), use_container_width=True)
                        xlsx_fac = make_output_excel(result_fac, sheet_name="Mis_Facilidades")
                        
                        col_dl1, col_dl2 = st.columns(2)
                        with col_dl1:
                            st.download_button(
                                label="⬇️ Descargar Excel Mis Facilidades",
                                data=xlsx_fac,
                                file_name=f"consolidado_mis_facilidades_{date.today().strftime('%Y%m%d')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_fac_masivo"
                            )
                        with col_dl2:
                            if st.button("📦 Generar ZIP con archivos MinIO", key="btn_fac_zip"):
                                with st.spinner("Descargando archivos desde MinIO..."):
                                    zip_bytes, log_df = download_minio_to_zip_by_contributor(
                                        out_rows,
                                        url_field="data",
                                        contributor_field="cuit_representado"
                                    )
                                st.success(f"ZIP generado: {len(log_df)} operaciones")
                                st.download_button(
                                    label="⬇️ Descargar ZIP de archivos",
                                    data=zip_bytes,
                                    file_name=f"mis_facilidades_archivos_{date.today().strftime('%Y%m%d')}.zip",
                                    mime="application/zip",
                                    key="download_fac_zip_files"
                                )
                                log_xlsx = make_output_excel(log_df, sheet_name="Log_Descargas")
                                st.download_button(
                                    label="📋 Descargar Log",
                                    data=log_xlsx,
                                    file_name=f"log_mis_facilidades_{date.today().strftime('%Y%m%d')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_fac_log"
                                )

    # -------------------------------------------------------------
    # TAB: Aportes en Línea
    # -------------------------------------------------------------
    with tab_aportes_linea:
        st.markdown("### Aportes en Línea")
        st.write("Consulta aportes y contribuciones en línea. Modo individual o masivo (Excel).")
        ap_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="ap_mode", horizontal=True)
        ap_minio = st.checkbox("Archivo histórico MinIO", value=True, key="ap_minio_option")
        ap_b64 = st.checkbox("Archivo histórico base64", value=False, key="ap_b64_option")
        ap_proxy = st.checkbox("Usar proxy_request", value=False, key="ap_proxy_option")
        
        if ap_mode == "Individual":
            ap_cuit_login = st.text_input("CUIT login", value="", key="ap_cuit_login_ind")
            ap_clave = st.text_input("Clave", value="", type="password", key="ap_clave_ind")
            ap_cuit_repr = st.text_input("CUIT representado (opcional)", value="", key="ap_cuit_repr_ind")
            
            if st.button("Consultar Aportes en Línea", key="btn_ap_consulta_ind"):
                if not (ap_cuit_login.strip() and ap_clave.strip()):
                    st.warning("Completá CUIT login y clave.")
                else:
                    headers_local = build_headers(x_api_key, header_email)
                    payload_ap = {
                        "cuit_login": ap_cuit_login.strip(),
                        "clave": ap_clave,
                        "cuit_representado": ap_cuit_repr.strip() if ap_cuit_repr.strip() else None,
                        "archivo_historico_b64": bool(ap_b64),
                        "archivo_historico_minio": bool(ap_minio),
                        "proxy_request": bool(ap_proxy)
                    }
                    with st.spinner("Consultando Aportes en Línea..."):
                        resp_ap = call_aportes_en_linea_consulta(base_url, headers_local, payload_ap)
                    st.info(f"HTTP status: {resp_ap.get('http_status')}")
                    st.json(resp_ap.get("data"))
                    cuit_id = ap_cuit_repr.strip() if ap_cuit_repr.strip() else ap_cuit_login.strip()
                    st.session_state["ap_last_response"] = resp_ap.get("data")
                    st.session_state["ap_last_cuit_id"] = cuit_id

            last_ap_data = st.session_state.get("ap_last_response")
            last_ap_cuit = st.session_state.get("ap_last_cuit_id", "").strip()
            if last_ap_data is not None and last_ap_cuit:
                # Botón para descargar ZIP con archivos de MinIO
                if st.button("📦 Generar ZIP con archivos MinIO", key="btn_ap_zip_ind"):
                    with st.spinner("Descargando archivos desde MinIO..."):
                        zip_bytes, log_df = download_minio_to_zip_by_contributor(
                            [{"cuit_representado": last_ap_cuit, "data": json.dumps(last_ap_data, ensure_ascii=False)}],
                            url_field="data",
                            contributor_field="cuit_representado"
                        )
                    if len(log_df) > 0:
                        st.success(f"ZIP generado: {len(log_df)} operaciones")
                        col1, col2 = st.columns(2)
                        with col1:
                            st.download_button(
                                label="⬇️ Descargar ZIP",
                                data=zip_bytes,
                                file_name=f"aportes_linea_{last_ap_cuit}_{date.today().strftime('%Y%m%d')}.zip",
                                mime="application/zip",
                                key="download_ap_zip_ind"
                            )
                        with col2:
                            log_xlsx = make_output_excel(log_df, sheet_name="Log")
                            st.download_button(
                                label="📋 Log",
                                data=log_xlsx,
                                file_name=f"log_aportes_linea_{last_ap_cuit}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_ap_log_ind"
                            )
                    else:
                        st.info("No se encontraron URLs de MinIO en la respuesta.")
        else:
            st.markdown("#### Consulta masiva Aportes en Línea")
            st.write("Subí un Excel con: **cuit_login**, **clave**, **cuit_representado** (opcional)")
            ap_file = st.file_uploader("Archivo Excel", type=["xlsx"], key="ap_file_upload")
            if ap_file:
                try:
                    df_ap = pd.read_excel(ap_file, dtype=str).fillna("")
                except Exception as e:
                    st.error(f"Error leyendo Excel: {e}")
                    df_ap = pd.DataFrame()
                df_ap.columns = [c.strip().lower() for c in df_ap.columns]
                required = ["cuit_login", "clave"]
                missing = [c for c in required if c not in df_ap.columns]
                if missing:
                    st.error(f"Faltan columnas: {', '.join(missing)}")
                else:
                    st.success(f"Filas leídas: {len(df_ap)}")
                    with st.expander("👀 Vista previa"):
                        st.dataframe(df_ap.head(10), use_container_width=True)
                    if st.button("Procesar Aportes en Línea masivo", key="btn_ap_masivo"):
                        headers_local = build_headers(x_api_key, header_email)
                        out_rows = []
                        progress = st.progress(0)
                        status_ph = st.empty()
                        for idx, row in df_ap.reset_index(drop=True).iterrows():
                            cuit_repr = row.get("cuit_representado", "").strip()
                            status_ph.info(f"Procesando {idx+1}/{len(df_ap)} — {row['cuit_login']}")
                            payload = {
                                "cuit_login": row["cuit_login"].strip(),
                                "clave": row["clave"],
                                "cuit_representado": cuit_repr if cuit_repr else None,
                                "archivo_historico_b64": bool(ap_b64),
                                "archivo_historico_minio": bool(ap_minio),
                                "proxy_request": bool(ap_proxy)
                            }
                            resp = call_aportes_en_linea_consulta(base_url, headers_local, payload)
                            out_rows.append({
                                "cuit_login": row["cuit_login"],
                                "cuit_representado": cuit_repr,
                                "http_status": resp.get("http_status"),
                                "data": json.dumps(resp.get("data"), ensure_ascii=False)
                            })
                            progress.progress(int((idx + 1) / len(df_ap) * 100))
                        status_ph.success("Procesamiento finalizado.")
                        result_ap = pd.DataFrame(out_rows)
                        st.dataframe(result_ap.head(50), use_container_width=True)
                        xlsx_ap = make_output_excel(result_ap, sheet_name="Aportes_Linea")
                        
                        col_dl1, col_dl2 = st.columns(2)
                        with col_dl1:
                            st.download_button(
                                label="⬇️ Descargar Excel Aportes en Línea",
                                data=xlsx_ap,
                                file_name=f"consolidado_aportes_linea_{date.today().strftime('%Y%m%d')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_ap_masivo"
                            )
                        with col_dl2:
                            if st.button("📦 Generar ZIP con archivos MinIO", key="btn_ap_zip"):
                                with st.spinner("Descargando archivos desde MinIO..."):
                                    zip_bytes, log_df = download_minio_to_zip_by_contributor(
                                        out_rows,
                                        url_field="data",
                                        contributor_field="cuit_representado"
                                    )
                                st.success(f"ZIP generado: {len(log_df)} operaciones")
                                st.download_button(
                                    label="⬇️ Descargar ZIP de archivos",
                                    data=zip_bytes,
                                    file_name=f"aportes_linea_archivos_{date.today().strftime('%Y%m%d')}.zip",
                                    mime="application/zip",
                                    key="download_ap_zip_files"
                                )
                                log_xlsx = make_output_excel(log_df, sheet_name="Log_Descargas")
                                st.download_button(
                                    label="📋 Descargar Log",
                                    data=log_xlsx,
                                    file_name=f"log_aportes_linea_{date.today().strftime('%Y%m%d')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_ap_log"
                                )

    # -------------------------------------------------------------
    # Subtab: Consulta Apócrifos (individual y masivo)
    # -------------------------------------------------------------
    with subtab_apoc:
        st.markdown("### Consulta de Apócrifos")
        st.write(
            "Verifica si uno o varios CUITs se encuentran en la base de apócrifos. "
            "Puedes realizar una consulta individual o cargar múltiples CUITs "
            "separados por comas, espacios o saltos de línea para una consulta masiva."
        )
        # Permite elegir entre modo individual y masivo.
        apoc_mode = st.radio(
            "Tipo de consulta", ["Individual", "Masiva"], key="apoc_mode", horizontal=True
        )
        if apoc_mode == "Individual":
            # Consulta individual de apócrifos
            apoc_cuit = st.text_input(
                "CUIT a consultar", value="", key="apoc_cuit_individual"
            )
            if st.button("Consultar Apócrifo individual", key="btn_apoc_consulta_ind"):
                if not apoc_cuit.strip():
                    st.warning("Ingresá un CUIT para consultar.")
                else:
                    headers_local = build_headers(x_api_key, header_email)
                    with st.spinner("Consultando apócrifo individual..."):
                        resp_apoc = call_apoc_consulta(base_url, headers_local, apoc_cuit.strip())
                    st.info(f"HTTP status: {resp_apoc.get('http_status')}")
                    # Muestra la respuesta tal cual la devuelve la API
                    st.json(resp_apoc.get("data"))
        else:
            # Consulta masiva de apócrifos basada en una lista de CUITs
            cuits_text_apoc = st.text_area(
                "Lista de CUITs (separados por comas, espacios o saltos de línea)",
                value="",
                height=150,
                key="apoc_cuits_masivo"
            )
            if st.button("Consultar Apócrifos masivos", key="btn_apoc_consulta_masivo"):
                # Procesar entrada de texto para obtener lista de CUITs
                raw = cuits_text_apoc.replace("\n", ",")
                cuits_list = [c.strip() for c in re.split(r",|\s", raw) if c.strip()]
                if not cuits_list:
                    st.warning("Ingresá al menos un CUIT para la consulta masiva.")
                else:
                    headers_local = build_headers(x_api_key, header_email)
                    # Preparar contenedores para resultados
                    out_rows_apoc: List[Dict[str, Any]] = []
                    status_ph = st.empty()
                    progress = st.progress(0)
                    with st.spinner("Consultando apócrifos masivos..."):
                        for idx, cuit in enumerate(cuits_list):
                            resp = call_apoc_consulta(base_url, headers_local, cuit)
                            http_status = resp.get("http_status")
                            data = resp.get("data")
                            # Intentar extraer campos específicos si la respuesta es un dict
                            es_apoc = None
                            fecha_apoc = None
                            fecha_publicacion = None
                            if isinstance(data, dict):
                                # Algunos campos comunes que podrían estar presentes en la respuesta
                                es_apoc = data.get("apoc") or data.get("es_apocrifo")
                                fecha_apoc = data.get("fecha_apoc") or data.get("fecha")
                                fecha_publicacion = data.get("fecha_publicacion")
                            out_rows_apoc.append({
                                "cuit": cuit,
                                "http_status": http_status,
                                "apoc": es_apoc,
                                "fecha_apoc": fecha_apoc,
                                "fecha_publicacion": fecha_publicacion,
                                "data": json.dumps(data, ensure_ascii=False)
                            })
                            # Actualizar progreso
                            progress.progress(int((idx + 1) / len(cuits_list) * 100))
                    status_ph.success("Procesamiento finalizado.")
                    # Mostrar DataFrame con los resultados
                    df_apoc = pd.DataFrame(out_rows_apoc)
                    st.write("### Resultado de consultas de Apócrifos (vista previa)")
                    st.dataframe(df_apoc.head(50), use_container_width=True)
                    # Permitir descarga del consolidado a Excel
                    xlsx_bytes_apoc = make_output_excel(df_apoc, sheet_name="Apoc_Masivo")
                    st.download_button(
                        label="⬇️ Descargar Excel de resultados Apócrifos",
                        data=xlsx_bytes_apoc,
                        file_name=f"consolidado_apoc_{date.today().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="download_apoc_masivo"
                    )

    # -------------------------------------------------------------
    # Subtab: Consulta de CUIT (individual y masivo)
    # -------------------------------------------------------------
    with subtab_cuit:
        st.markdown("### Consulta de CUIT")
        st.write(
            "Obtén la constancia de inscripción de uno o varios CUITs. Puedes realizar una consulta individual "
            "o cargar múltiples CUITs separados por comas o saltos de línea para una consulta masiva."
        )
        mode = st.radio("Tipo de consulta", ["Individual", "Masiva"], key="cuit_mode", horizontal=True)
        if mode == "Individual":
            cuit_individual = st.text_input("CUIT individual", value="", key="cuit_individual")
            if st.button("Consultar CUIT individual", key="btn_cuit_individual"):
                if not cuit_individual.strip():
                    st.warning("Ingresá el CUIT a consultar.")
                else:
                    headers_local = build_headers(x_api_key, header_email)
                    payload_cuit_ind = {"cuit": cuit_individual.strip()}
                    with st.spinner("Consultando CUIT individual..."):
                        resp_cuit_ind = call_cuit_individual(base_url, headers_local, payload_cuit_ind)
                    st.info(f"HTTP status: {resp_cuit_ind.get('http_status')}")
                    st.json(resp_cuit_ind.get("data"))
        else:
            cuits_text = st.text_area(
                "Lista de CUITs (separados por comas, espacios o saltos de línea)",
                value="",
                height=150,
                key="cuits_masivo"
            )
            if st.button("Consultar CUITs masivos", key="btn_cuit_masivo"):
                # Procesar entrada para obtener lista de CUITs
                raw = cuits_text.replace("\n", ",")
                cuits_list = [c.strip() for c in re.split(r",|\s", raw) if c.strip()]
                if not cuits_list:
                    st.warning("Ingresá al menos un CUIT para la consulta masiva.")
                else:
                    headers_local = build_headers(x_api_key, header_email)
                    payload_cuit_mass = {"cuits": cuits_list}
                    with st.spinner("Consultando CUITs masivos..."):
                        resp_cuit_mass = call_cuit_masivo(base_url, headers_local, payload_cuit_mass)
                    st.info(f"HTTP status: {resp_cuit_mass.get('http_status')}")
                    st.json(resp_cuit_mass.get("data"))
