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
st.set_page_config(page_title="BOTs de Mrbot", page_icon="🧾", layout="wide")
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

def download_to_zip(urls_emitidos: List[str], urls_recibidos: List[str], timeout_sec: int = 120) -> Tuple[bytes, pd.DataFrame]:
    log_rows = []
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        def process_list(urls, carpeta, tipo):
            for url in urls:
                if not url or not isinstance(url, str):
                    continue
                try:
                    r = requests.get(url, timeout=timeout_sec, stream=True)
                    if r.status_code != 200:
                        log_rows.append({"tipo": tipo, "url": url, "estado": "error_http", "detalle": f"HTTP {r.status_code}"})
                        continue
                    fname = get_filename_from_headers(r) or infer_filename_from_url(url)
                    ctype = r.headers.get("Content-Type", "")
                    content = r.content
                    if is_zip_bytes(content, ctype, fname):
                        try:
                            with zipfile.ZipFile(BytesIO(content)) as inzip:
                                had_file = False
                                for zi in inzip.infolist():
                                    if zi.is_dir():
                                        continue
                                    try:
                                        raw = inzip.read(zi.filename)
                                    except Exception as e:
                                        log_rows.append({"tipo": tipo, "url": url, "estado": "error_lectura_zip", "detalle": f"{zi.filename}: {e}"})
                                        continue
                                    inner_name = sanitize_filename(os.path.basename(zi.filename)) or "archivo"
                                    arcname = os.path.join(carpeta, inner_name)
                                    final_name = write_unique(zf, arcname, raw)
                                    had_file = True
                                    log_rows.append({"tipo": tipo, "url": url, "estado": "ok_extraido", "detalle": final_name})
                                if not had_file:
                                    log_rows.append({"tipo": tipo, "url": url, "estado": "zip_vacio", "detalle": fname})
                        except zipfile.BadZipFile:
                            arcname = os.path.join(carpeta, fname or "archivo")
                            final_name = write_unique(zf, arcname, content)
                            log_rows.append({"tipo": tipo, "url": url, "estado": "ok_archivo", "detalle": final_name})
                    else:
                        arcname = os.path.join(carpeta, fname or "archivo")
                        final_name = write_unique(zf, arcname, content)
                        log_rows.append({"tipo": tipo, "url": url, "estado": "ok_archivo", "detalle": final_name})
                except Exception as e:
                    log_rows.append({"tipo": tipo, "url": url, "estado": "error", "detalle": str(e)})
        process_list(urls_emitidos, "Emitidos", "emitido")
        process_list(urls_recibidos, "Recibidos", "recibido")
    zip_buffer.seek(0)
    return zip_buffer.read(), pd.DataFrame(log_rows)

# =========================
# NUEVAS UTILIDADES — SOLAPA 4
# =========================
CUIT_REGEX = re.compile(r"(?<!\d)(\d{11})(?!\d)")

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
tab_users, tab_mis_comprobantes, tab_other1, tab_other2, tab_other3, tab_other4, tab_other5 = st.tabs([
    "Usuarios",
    "Mis Comprobantes",
    "RCEL",
    "SCT",
    "CCMA",
    "APOC",
    "Consulta de Constancia de CUIT"
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
# TAB 3: Descarga de archivos S3/MinIO a ZIP (extrayendo .zip internos)
# -------------------------------------------------------------------
with tab3:
    st.subheader("3) Descargar columnas MinIO del consolidado → ZIP (Emitidos/Recibidos)")
    st.write(
        "Subí el **Excel consolidado** de la solapa 1. Se leerán preferentemente las columnas `emitidos_url_minio` y `recibidos_url_minio`. "
        "Si no existen, se intentará usar las columnas de S3. Si una URL es un `.zip`, "
        "**se extraen sus archivos internos** y se guardan sueltos en las carpetas correspondientes."
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

        # Extraer listas de URLs según la prioridad MinIO -> S3
        if col_emitidos_minio:
            urls_emitidos = [u for u in df_zip[col_emitidos_minio].tolist() if isinstance(u, str) and u.strip()]
        else:
            urls_emitidos = [u for u in df_zip[col_emitidos_s3].tolist() if isinstance(u, str) and u.strip()]
        if col_recibidos_minio:
            urls_recibidos = [u for u in df_zip[col_recibidos_minio].tolist() if isinstance(u, str) and u.strip()]
        else:
            urls_recibidos = [u for u in df_zip[col_recibidos_s3].tolist() if isinstance(u, str) and u.strip()]

        st.write(f"URLs en **Emitidos**: {len(urls_emitidos)} | URLs en **Recibidos**: {len(urls_recibidos)}")

        if st.button("📦 Generar ZIP con descargas (extrayendo .zip)", key="btn_zip"):
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

    st.caption("Nota: Si un enlace apunta a un `.zip`, se extraen sus contenidos y se guardan sueltos dentro de `Emitidos/` o `Recibidos/`. No se incluye el `.zip` original.")

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
with tab_other1:
    st.subheader("RCEL")
    subtab_rcel = tab_other1  # Reutilizar variable para el contenido existente
with tab_other2:
    st.subheader("SCT")
    subtab_sct = tab_other2
with tab_other3:
    st.subheader("CCMA")
    subtab_ccma = tab_other3
with tab_other4:
    st.subheader("APOC")
    subtab_apoc = tab_other4
with tab_other5:
    st.subheader("Consulta de Constancia de CUIT")
    subtab_cuit = tab_other5

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
        else:
            st.markdown("#### Consulta masiva RCEL")
            st.write(
                "Subí un archivo Excel (.xlsx) con las columnas **cuit_representante**, **nombre_rcel**, "
                "**representado_cuit** y **clave**. Para cada fila se enviará una solicitud."
            )
            rcel_file = st.file_uploader("Archivo Excel con contribuyentes", type=["xlsx"], key="rcel_file_upload")
            if rcel_file is not None:
                try:
                    df_rcel = pd.read_excel(rcel_file, dtype=str).fillna("")
                except Exception as e:
                    st.error(f"Error leyendo el Excel: {e}")
                    df_rcel = pd.DataFrame()
                required_cols = ["cuit_representante", "nombre_rcel", "representado_cuit", "clave"]
                df_rcel.columns = [c.strip().lower() for c in df_rcel.columns]
                missing = [c for c in required_cols if c not in df_rcel.columns]
                if missing:
                    st.error(f"El Excel cargado no tiene las columnas requeridas: {', '.join(missing)}")
                else:
                    st.success(f"Filas leídas: {len(df_rcel)}")
                    with st.expander("👀 Vista previa (primeras filas)"):
                        st.dataframe(df_rcel.head(10), use_container_width=True)
                    if st.button("Procesar consultas RCEL", key="btn_rcel_masivo"):
                        headers_local = build_headers(x_api_key, header_email)
                        out_rows = []
                        progress = st.progress(0)
                        status_ph = st.empty()
                        for idx, row in df_rcel.reset_index(drop=True).iterrows():
                            status_ph.info(
                                f"Procesando {idx+1}/{len(df_rcel)} — {row['nombre_rcel']} (CUIT {row['representado_cuit']})"
                            )
                            payload = {
                                "desde": as_ddmmyyyy(rcel_desde),
                                "hasta": as_ddmmyyyy(rcel_hasta),
                                "cuit_representante": row["cuit_representante"].strip(),
                                "nombre_rcel": row["nombre_rcel"].strip(),
                                "representado_cuit": row["representado_cuit"].strip(),
                                "clave": row["clave"],
                                "b64_pdf": bool(rcel_b64_pdf),
                                "minio_upload": bool(rcel_minio)
                            }
                            resp = call_rcel_consulta(base_url, headers_local, payload)
                            http_status = resp.get("http_status")
                            data = resp.get("data", {})
                            # Extraer algunos campos de interés para el resumen
                            success = data.get("success") if isinstance(data, dict) else None
                            message = data.get("message") if isinstance(data, dict) else None
                            # Contar cantidad de facturas devueltas
                            num_facturas = None
                            if isinstance(data, dict):
                                fe = data.get("facturas_emitidas")
                                if isinstance(fe, list):
                                    num_facturas = len(fe)
                            out_rows.append({
                                "cuit_representante": row["cuit_representante"],
                                "nombre_rcel": row["nombre_rcel"],
                                "representado_cuit": row["representado_cuit"],
                                "http_status": http_status,
                                "success": success,
                                "message": message,
                                "num_facturas": num_facturas
                            })
                            progress.progress(int((idx + 1) / len(df_rcel) * 100))
                        status_ph.success("Procesamiento finalizado.")
                        result_rcel_df = pd.DataFrame(out_rows)
                        st.write("### Resultado de consultas RCEL (vista previa)")
                        st.dataframe(result_rcel_df.head(50), use_container_width=True)
                        xlsx_bytes = make_output_excel(result_rcel_df, sheet_name="RCEL_Masivo")
                        st.download_button(
                            label="⬇️ Descargar Excel de resultados RCEL",
                            data=xlsx_bytes,
                            file_name=f"consolidado_rcel_{date.today().strftime('%Y%m%d')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="download_rcel_masivo"
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
        else:
            st.markdown("#### Consulta masiva SCT")
            st.write(
                "Subí un archivo Excel (.xlsx) con las columnas **cuit_login**, **clave** y **cuit_representado**. "
                "Para cada fila se enviará una solicitud."
            )
            sct_file = st.file_uploader("Archivo Excel con contribuyentes", type=["xlsx"], key="sct_file_upload")
            if sct_file is not None:
                try:
                    df_sct = pd.read_excel(sct_file, dtype=str).fillna("")
                except Exception as e:
                    st.error(f"Error leyendo el Excel: {e}")
                    df_sct = pd.DataFrame()
                required_cols_sct = ["cuit_login", "clave", "cuit_representado"]
                df_sct.columns = [c.strip().lower() for c in df_sct.columns]
                missing = [c for c in required_cols_sct if c not in df_sct.columns]
                if missing:
                    st.error(f"El Excel cargado no tiene las columnas requeridas: {', '.join(missing)}")
                else:
                    st.success(f"Filas leídas: {len(df_sct)}")
                    with st.expander("👀 Vista previa (primeras filas)"):
                        st.dataframe(df_sct.head(10), use_container_width=True)
                    if st.button("Procesar consultas SCT", key="btn_sct_masivo"):
                        headers_local = build_headers(x_api_key, header_email)
                        out_rows_sct = []
                        progress = st.progress(0)
                        status_ph = st.empty()
                        for idx, row in df_sct.reset_index(drop=True).iterrows():
                            status_ph.info(
                                f"Procesando {idx+1}/{len(df_sct)} — CUIT {row['cuit_representado']}"
                            )
                            payload_sct = {
                                "cuit_login": row["cuit_login"].strip(),
                                "clave": row["clave"],
                                "cuit_representado": row["cuit_representado"].strip(),
                                "excel_b64": bool(sct_excel_b64),
                                "csv_b64": bool(sct_csv_b64),
                                "pdf_b64": bool(sct_pdf_b64),
                                "excel_minio": bool(sct_excel_minio),
                                "csv_minio": bool(sct_csv_minio),
                                "pdf_minio": bool(sct_pdf_minio),
                                "proxy_request": bool(sct_proxy)
                            }
                            resp = call_sct_consulta(base_url, headers_local, payload_sct)
                            http_status = resp.get("http_status")
                            data = resp.get("data", {})
                            status_field = None
                            error_message = None
                            if isinstance(data, dict):
                                status_field = data.get("status")
                                error_message = data.get("error_message")
                            out_rows_sct.append({
                                "cuit_login": row["cuit_login"],
                                "cuit_representado": row["cuit_representado"],
                                "http_status": http_status,
                                "status": status_field,
                                "error_message": error_message
                            })
                            progress.progress(int((idx + 1) / len(df_sct) * 100))
                        status_ph.success("Procesamiento finalizado.")
                        result_sct_df = pd.DataFrame(out_rows_sct)
                        st.write("### Resultado de consultas SCT (vista previa)")
                        st.dataframe(result_sct_df.head(50), use_container_width=True)
                        xlsx_bytes = make_output_excel(result_sct_df, sheet_name="SCT_Masivo")
                        st.download_button(
                            label="⬇️ Descargar Excel de resultados SCT",
                            data=xlsx_bytes,
                            file_name=f"consolidado_sct_{date.today().strftime('%Y%m%d')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="download_sct_masivo"
                        )

    # -------------------------------------------------------------
    # Subtab: CCMA (Cuenta Corriente de Monotributistas y Autónomos)
    # -------------------------------------------------------------
    with subtab_ccma:
        st.markdown("### Cuenta Corriente de Monotributistas y Autónomos (CCMA)")
        st.write(
            "Consulta la cuenta corriente de uno o varios contribuyentes. "
            "Seleccioná el modo de consulta individual o masivo."
        )
        ccma_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="ccma_mode")
        ccma_proxy = st.checkbox("Usar proxy_request", value=False, key="ccma_proxy_option")
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
                        "proxy_request": bool(ccma_proxy)
                    }
                    with st.spinner("Consultando CCMA..."):
                        resp_ccma = call_ccma_consulta(base_url, headers_local, payload_ccma)
                    st.info(f"HTTP status: {resp_ccma.get('http_status')}")
                    st.json(resp_ccma.get("data"))
        else:
            st.markdown("#### Consulta masiva CCMA")
            st.write(
                "Subí un archivo Excel (.xlsx) con las columnas **cuit_representante**, **clave_representante** y "
                "**cuit_representado**. Para cada fila se enviará una solicitud."
            )
            ccma_file = st.file_uploader("Archivo Excel con contribuyentes", type=["xlsx"], key="ccma_file_upload")
            if ccma_file is not None:
                try:
                    df_ccma = pd.read_excel(ccma_file, dtype=str).fillna("")
                except Exception as e:
                    st.error(f"Error leyendo el Excel: {e}")
                    df_ccma = pd.DataFrame()
                required_cols_ccma = ["cuit_representante", "clave_representante", "cuit_representado"]
                df_ccma.columns = [c.strip().lower() for c in df_ccma.columns]
                missing = [c for c in required_cols_ccma if c not in df_ccma.columns]
                if missing:
                    st.error(f"El Excel cargado no tiene las columnas requeridas: {', '.join(missing)}")
                else:
                    st.success(f"Filas leídas: {len(df_ccma)}")
                    with st.expander("👀 Vista previa (primeras filas)"):
                        st.dataframe(df_ccma.head(10), use_container_width=True)
                    if st.button("Procesar consultas CCMA", key="btn_ccma_masivo"):
                        headers_local = build_headers(x_api_key, header_email)
                        out_rows_ccma = []
                        progress = st.progress(0)
                        status_ph = st.empty()
                        for idx, row in df_ccma.reset_index(drop=True).iterrows():
                            status_ph.info(
                                f"Procesando {idx+1}/{len(df_ccma)} — CUIT {row['cuit_representado']}"
                            )
                            payload_ccma = {
                                "cuit_representante": row["cuit_representante"].strip(),
                                "clave_representante": row["clave_representante"],
                                "cuit_representado": row["cuit_representado"].strip(),
                                "proxy_request": bool(ccma_proxy)
                            }
                            resp = call_ccma_consulta(base_url, headers_local, payload_ccma)
                            http_status = resp.get("http_status")
                            data = resp.get("data", {})
                            status_field = None
                            error_message = None
                            if isinstance(data, dict):
                                status_field = data.get("status")
                                error_message = data.get("error_message")
                            out_rows_ccma.append({
                                "cuit_representante": row["cuit_representante"],
                                "cuit_representado": row["cuit_representado"],
                                "http_status": http_status,
                                "status": status_field,
                                "error_message": error_message
                            })
                            progress.progress(int((idx + 1) / len(df_ccma) * 100))
                        status_ph.success("Procesamiento finalizado.")
                        result_ccma_df = pd.DataFrame(out_rows_ccma)
                        st.write("### Resultado de consultas CCMA (vista previa)")
                        st.dataframe(result_ccma_df.head(50), use_container_width=True)
                        xlsx_bytes = make_output_excel(result_ccma_df, sheet_name="CCMA_Masivo")
                        st.download_button(
                            label="⬇️ Descargar Excel de resultados CCMA",
                            data=xlsx_bytes,
                            file_name=f"consolidado_ccma_{date.today().strftime('%Y%m%d')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="download_ccma_masivo"
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
