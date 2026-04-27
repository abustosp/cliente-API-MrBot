# app_mis_comprobantes_tabs.py — clientes/Cliente_API_Mrbot_streamlit
import streamlit as st
import pandas as pd
import json
import re
import zipfile
import base64
import os
from io import BytesIO
from datetime import date
from typing import Any, Dict, Optional, List

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# === imports modulares (api + utils) ===
from api import ensure_trailing_slash, build_headers, safe_post, safe_get
from api.endpoints import (
    call_consulta, call_consultas_disponibles,
    call_rcel_consulta, call_sct_consulta, call_ccma_consulta, call_apoc_consulta,
    call_cuit_individual, call_cuit_masivo,
    call_mis_retenciones_consulta, call_sifere_consulta,
    call_declaracion_en_linea_consulta, call_mis_facilidades_consulta,
    call_aportes_en_linea_consulta,
    call_create_user_api, call_reset_api_key,
    call_pago_devoluciones_consulta, call_hacienda_consulta,
    call_liquidacion_granos_consulta,
    call_portal_iva_consulta,
    call_arba_consulta, call_agip_consulta, call_misiones_consulta,
    call_srt_alicuotas_consulta,
    run_bcra_operation, flatten_bcra_results, get_bcra_operation_choices,
    call_procesar_pem, build_pem_excel,
)
from utils import (
    make_output_excel, pick_url_fields,
    download_to_zip, download_minio_to_zip_by_contributor, download_minio_links_to_zip,
    extract_minio_urls_from_excel, collect_url_entries_from_df,
    normalize_ccma_response, build_ccma_outputs, build_ccma_excel,
    consolidate_group_from_zip, build_zip_with_excels,
    render_minio_mass_download, as_ddmmyyyy,
    parse_amount,
)
from utils.file_utils import sanitize_filename

# =========================
# CONFIGURACIÓN BÁSICA UI
# =========================
st.set_page_config(page_title="BOTs de Mrbot", page_icon="static/ABP.png", layout="wide")
st.title("BOTs de Mrbot")
st.caption("Consultas masivas, estado de consultas, descarga desde S3/MinIO y consolidacion final de archivos.")

# =========================
# PARAMETROS GLOBALES (Sidebar)
# =========================
with st.sidebar:
    st.header("Conexion")
    default_api_key = os.getenv("X_API_KEY", "")
    default_email = os.getenv("EMAIL", "")
    base_url = st.text_input(
        "Base URL de la API",
        value="https://api-bots.mrbot.com.ar/",
        help="Ej.: https://api-bots.mrbot.com.ar/ (debe terminar con /)"
    )
    x_api_key = st.text_input("x-api-key (opcional, header)", value=default_api_key, type="password")
    header_email = st.text_input("email (opcional, header)", value=default_email)

REQUIRED_COLS = ["cuit_inicio_sesion", "nombre_representado", "cuit_representado", "contrasena"]

# =========================
# TABS (nueva estructura)
# =========================
(tab_users, tab_mis_comprobantes, tab_rcel, tab_sct, tab_ccma, tab_mis_retenciones,
 tab_sifere, tab_declaracion_linea, tab_mis_facilidades, tab_aportes_linea,
 tab_apoc, tab_cuit,
 tab_pago_devoluciones, tab_hacienda, tab_liquidacion_granos,
 tab_portal_iva, tab_ret_provinciales, tab_srt_alicuotas,
 tab_bcra, tab_procesar_pem) = st.tabs([
    "Usuarios",
    "Mis Comprobantes",
    "RCEL",
    "SCT",
    "CCMA",
    "Mis Retenciones",
    "SIFERE",
    "Declaracion en Linea",
    "Mis Facilidades",
    "Aportes en Linea",
    "APOC",
    "Consulta de CUIT",
    "Pago y Devoluciones",
    "Hacienda",
    "Liquidacion Granos",
    "Portal IVA",
    "Ret. Provinciales",
    "SRT Alicuotas",
    "BCRA",
    "Procesar PEM",
])

# --- Mis Comprobantes sub-tabs ---
with tab_mis_comprobantes:
    subtab_mc_consulta, subtab_mc_descarga_zip, subtab_mc_consolidar = st.tabs([
        "Consulta masiva", "Descargar ZIP", "Consolidar salidas"
    ])

# --- Usuarios sub-tabs ---
with tab_users:
    subtab_user_create, subtab_user_reset, subtab_user_consultas = st.tabs([
        "Crear usuario", "Resetear API key", "Consultas disponibles"
    ])

tab1 = subtab_mc_consulta
tab2 = subtab_user_consultas
tab3 = subtab_mc_descarga_zip
tab4 = subtab_mc_consolidar

# ====================================================================
# TAB: Usuarios — Crear usuario
# ====================================================================
with subtab_user_create:
    st.subheader("Crear usuario")
    st.write("Crear un nuevo usuario y enviarle la API key por correo.")
    user_email_create = st.text_input("Email para crear usuario", value="", key="create_user_email")
    if st.button("Crear usuario", key="btn_create_user"):
        if not user_email_create.strip():
            st.warning("Ingresa un email valido.")
        else:
            payload_create = {"mail": user_email_create.strip()}
            with st.spinner("Creando usuario..."):
                resp_create = call_create_user_api(base_url, payload_create)
            st.info(f"HTTP status: {resp_create.get('http_status')}")
            st.json(resp_create.get("data"))

# --- Usuarios: Resetear API key ---
with subtab_user_reset:
    st.subheader("Resetear API key")
    st.write("Restablece la API key de un usuario y envia la nueva clave por correo.")
    user_email_reset = st.text_input("Email para resetear API key", value="", key="reset_user_email")
    if st.button("Resetear API key", key="btn_reset_api_key"):
        if not user_email_reset.strip():
            st.warning("Ingresa un email valido.")
        else:
            payload_reset = {"mail": user_email_reset.strip()}
            with st.spinner("Reseteando API key..."):
                resp_reset = call_reset_api_key(base_url, payload_reset)
            st.info(f"HTTP status: {resp_reset.get('http_status')}")
            st.json(resp_reset.get("data"))

# ====================================================================
# TAB 1: Consulta Masiva (Mis Comprobantes)
# ====================================================================
with tab1:
    st.subheader("1) Consulta masiva a /api/v1/mis_comprobantes/consulta")
    with st.expander("Parametros de consulta", expanded=True):
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
            proxy_request = st.toggle("Usar proxy_request", value=False)
        st.caption("Opciones de carga de archivos (API v1)")
        col_c1, col_c2, col_c3 = st.columns(3)
        with col_c1:
            carga_s3 = st.checkbox("Subir a S3", value=False)
        with col_c2:
            carga_minio = st.checkbox("Subir a MinIO", value=True)
        with col_c3:
            carga_json = st.checkbox("Recibir JSON", value=False)
    st.markdown("### Cargar archivo Excel (credenciales por representado)")
    st.write("El Excel debe contener exactamente estas columnas:")
    st.code("cuit_inicio_sesion, nombre_representado, cuit_representado, contrasena", language="text")
    uploaded = st.file_uploader("Selecciona el archivo .xlsx", type=["xlsx"], key="uploader_tab1")
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
        with st.expander("Vista previa (primeras filas)"):
            st.dataframe(input_df.head(10), use_container_width=True)
        if st.button("Procesar consultas y generar consolidado", key="procesar_tab1"):
            if len(input_df) == 0:
                st.warning("No hay filas validas para procesar.")
                st.stop()
            headers = build_headers(x_api_key, header_email)
            out_rows = []
            progress = st.progress(0)
            status_ph = st.empty()
            for idx, row in input_df.reset_index(drop=True).iterrows():
                status_ph.info(f"Procesando {idx+1}/{len(input_df)} - {row['nombre_representado']} (CUIT {row['cuit_representado']})")
                payload = {
                    "desde": as_ddmmyyyy(desde), "hasta": as_ddmmyyyy(hasta),
                    "cuit_inicio_sesion": row["cuit_inicio_sesion"].strip(),
                    "representado_nombre": row["nombre_representado"].strip(),
                    "representado_cuit": row["cuit_representado"].strip(),
                    "contrasena": row["contrasena"],
                    "descarga_emitidos": bool(descarga_emitidos),
                    "descarga_recibidos": bool(descarga_recibidos),
                    "proxy_request": bool(proxy_request),
                    "carga_s3": bool(carga_s3), "carga_minio": bool(carga_minio),
                    "carga_json": bool(carga_json), "b64": False
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
                    "http_status": http_status, "success": success, "message": message,
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
            st.markdown("### Consolidado de URLs (vista previa)")
            st.dataframe(result_df.head(50), use_container_width=True)
            xlsx_bytes = make_output_excel(result_df, sheet_name="Consolidado_URLs")
            st.download_button(
                label="Descargar Excel Consolidado", data=xlsx_bytes,
                file_name=f"consolidado_mis_comprobantes_{date.today().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_consolidado_tab1"
            )
            st.caption("Nota: No se almacenan contrasenas ni datos del Excel en el servidor.")

# ====================================================================
# TAB 2: Consultas disponibles
# ====================================================================
with tab2:
    st.subheader("2) Consultar cantidad de consultas disponibles")
    st.write("Consulta el endpoint **GET** `/api/v1/user/consultas/{email}`.")
    q_email = st.text_input("Email (path param)", value=header_email or "", help="Se usa como parte de la URL.")
    headers = build_headers(x_api_key, header_email)
    if st.button("Consultar", key="btn_consultas_disponibles"):
        if not q_email.strip():
            st.warning("Ingresa un email para consultar.")
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

# ====================================================================
# TAB 3: Descarga ZIP
# ====================================================================
with tab3:
    st.subheader("3) Descargar columnas MinIO del consolidado -> ZIP (Emitidos/Recibidos)")
    st.write(
        "Subi el **Excel consolidado** de la solapa 1. Se leen preferentemente las columnas `emitidos_url_minio` y `recibidos_url_minio`. "
        "Si no existen, se intentara usar las columnas de S3."
    )
    up_zip = st.file_uploader("Seleccionar consolidado (.xlsx)", type=["xlsx"], key="uploader_tab3")
    with st.expander("Opciones de descarga"):
        timeout_zip = st.number_input("Timeout por archivo (segundos)", min_value=10, value=120, step=10)
    if up_zip is not None:
        try:
            df_zip = pd.read_excel(up_zip, dtype=str).fillna("")
        except Exception as e:
            st.error(f"Error leyendo el Excel: {e}")
            st.stop()
        df_zip.columns = [c.strip().lower() for c in df_zip.columns]
        col_emitidos_minio = "emitidos_url_minio" if "emitidos_url_minio" in df_zip.columns else None
        col_recibidos_minio = "recibidos_url_minio" if "recibidos_url_minio" in df_zip.columns else None
        col_emitidos_s3 = "emitidos_url_s3" if "emitidos_url_s3" in df_zip.columns else None
        col_recibidos_s3 = "recibidos_url_s3" if "recibidos_url_s3" in df_zip.columns else None
        if not (col_emitidos_minio or col_emitidos_s3) or not (col_recibidos_minio or col_recibidos_s3):
            st.error("El Excel no posee las columnas requeridas.")
            st.stop()
        contrib_col = None
        for cand in ("cuit_representado", "representado_cuit"):
            if cand in df_zip.columns:
                contrib_col = cand
                break
        urls_emitidos = collect_url_entries_from_df(df_zip, col_emitidos_minio or col_emitidos_s3, contrib_col, extract_zip=not bool(col_emitidos_minio))
        urls_recibidos = collect_url_entries_from_df(df_zip, col_recibidos_minio or col_recibidos_s3, contrib_col, extract_zip=not bool(col_recibidos_minio))
        st.write(f"URLs en **Emitidos**: {len(urls_emitidos)} | URLs en **Recibidos**: {len(urls_recibidos)}")
        if st.button("Generar ZIP con descargas", key="btn_zip"):
            with st.spinner("Descargando archivos y construyendo ZIP..."):
                zip_bytes, log_df = download_to_zip(urls_emitidos=urls_emitidos, urls_recibidos=urls_recibidos, timeout_sec=int(timeout_zip))
            st.download_button(label="Descargar ZIP (Emitidos/Recibidos)", data=zip_bytes, file_name=f"descargas_{date.today().strftime('%Y%m%d')}.zip", mime="application/zip", key="download_zip_tab3")
            log_xlsx = make_output_excel(log_df, sheet_name="LogDescargas")
            st.download_button(label="Descargar Log (Excel)", data=log_xlsx, file_name=f"log_descargas_{date.today().strftime('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_log_tab3")
    st.caption("Nota: los links MinIO se guardan tal cual (sin extraccion).")

# ====================================================================
# TAB 4: Consolidar salidas
# ====================================================================
with tab4:
    st.subheader("4) Consolidar archivos de salida (ZIP -> 2 Excel)")
    st.write("Importa el **ZIP** con las carpetas `Emitidos/` y `Recibidos/` (CSV con separador `;`).")
    zip_in = st.file_uploader("Selecciona el ZIP con `Emitidos/` y `Recibidos/`", type=["zip"], key="uploader_tab4")
    if zip_in is not None and st.button("Consolidar ZIP -> 2 Excel (descargar ZIP)", key="btn_consolidar_zip"):
        try:
            with zipfile.ZipFile(zip_in) as zf:
                df_emitidos = consolidate_group_from_zip(zf, "Emitidos")
                df_recibidos = consolidate_group_from_zip(zf, "Recibidos")
        except zipfile.BadZipFile:
            st.error("El archivo subido no es un ZIP valido.")
            st.stop()
        except Exception as e:
            st.error(f"Error procesando el ZIP: {e}")
            st.stop()
        out_zip_bytes = build_zip_with_excels(df_emitidos, df_recibidos)
        st.download_button(label="Descargar ZIP con Consolidados (Emitidos/Recibidos)", data=out_zip_bytes, file_name=f"Consolidados_{date.today().strftime('%Y%m%d')}.zip", mime="application/zip", key="download_consolidados_zip")

# ====================================================================
# TAB: RCEL
# ====================================================================
with tab_rcel:
    st.subheader("RCEL")
    st.markdown("### Comprobantes en Linea (RCEL)")
    st.write("Consulta facturas emitidas en el servicio Comprobantes en Linea.")
    rcel_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="rcel_mode")
    rcel_desde = st.date_input("Desde", value=date(date.today().year, 1, 1), format="DD/MM/YYYY", key="rcel_desde_date")
    rcel_hasta = st.date_input("Hasta", value=date.today(), format="DD/MM/YYYY", key="rcel_hasta_date")
    rcel_b64_pdf = st.checkbox("PDF en base64", value=False, key="rcel_b64_pdf")
    rcel_minio = st.checkbox("Subir PDF a MinIO", value=True, key="rcel_minio_option")
    if rcel_mode == "Individual":
        rc_cuit_rep = st.text_input("CUIT del representante", value="", key="rcel_cuit_rep_ind")
        rc_nombre = st.text_input("Nombre exacto del contribuyente (nombre_rcel)", value="", key="rcel_nombre_ind")
        rc_cuit_repr = st.text_input("CUIT del contribuyente representado", value="", key="rcel_cuit_repr_ind")
        rc_clave = st.text_input("Clave fiscal", value="", type="password", key="rcel_clave_ind")
        if st.button("Consultar RCEL", key="btn_rcel_consulta_ind"):
            if not (rc_cuit_rep.strip() and rc_nombre.strip() and rc_cuit_repr.strip() and rc_clave.strip()):
                st.warning("Completa todos los campos obligatorios.")
            else:
                headers_local = build_headers(x_api_key, header_email)
                payload_rcel = {
                    "desde": as_ddmmyyyy(rcel_desde), "hasta": as_ddmmyyyy(rcel_hasta),
                    "cuit_representante": rc_cuit_rep.strip(), "nombre_rcel": rc_nombre.strip(),
                    "representado_cuit": rc_cuit_repr.strip(), "clave": rc_clave,
                    "b64_pdf": bool(rcel_b64_pdf), "minio_upload": bool(rcel_minio)
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
            if st.button("Generar ZIP con archivos MinIO", key="btn_rcel_zip_ind"):
                with st.spinner("Descargando archivos desde MinIO..."):
                    zip_bytes, log_df = download_minio_to_zip_by_contributor(
                        [{"representado_cuit": last_rcel_cuit, "data": json.dumps(last_rcel_data, ensure_ascii=False)}],
                        url_field="data", contributor_field="representado_cuit"
                    )
                if len(log_df) > 0:
                    st.success(f"ZIP generado: {len(log_df)} operaciones")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.download_button(label="Descargar ZIP", data=zip_bytes, file_name=f"rcel_{last_rcel_cuit}_{date.today().strftime('%Y%m%d')}.zip", mime="application/zip", key="download_rcel_zip_ind")
                    with col2:
                        log_xlsx = make_output_excel(log_df, sheet_name="Log")
                        st.download_button(label="Log", data=log_xlsx, file_name=f"log_rcel_{last_rcel_cuit}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_rcel_log_ind")
                else:
                    st.info("No se encontraron URLs de MinIO en la respuesta.")
    else:
        render_minio_mass_download(section_title="RCEL", uploader_key="rcel_minio_file_upload", timeout_key="rcel_minio_timeout", button_key="btn_rcel_minio_zip")

# ====================================================================
# TAB: SCT
# ====================================================================
with tab_sct:
    st.subheader("SCT")
    st.markdown("### Sistema de Cuentas Tributarias (SCT)")
    st.write("Consulta el estado del Sistema de Cuentas Tributarias.")
    sct_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="sct_mode")
    st.caption("Selecciona en que formatos queres recibir los archivos.")
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
                st.warning("Completa todos los campos obligatorios.")
            else:
                headers_local = build_headers(x_api_key, header_email)
                payload_sct = {
                    "cuit_login": sct_cuit_login.strip(), "clave": sct_clave,
                    "cuit_representado": sct_cuit_repr.strip(),
                    "excel_b64": bool(sct_excel_b64), "csv_b64": bool(sct_csv_b64),
                    "pdf_b64": bool(sct_pdf_b64), "excel_minio": bool(sct_excel_minio),
                    "csv_minio": bool(sct_csv_minio), "pdf_minio": bool(sct_pdf_minio),
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
                    for key_b64, ext, mime in [("excel_b64",".xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),("csv_b64",".csv","text/csv"),("pdf_b64",".pdf","application/pdf")]:
                        b64_val = data_sct.get(key_b64)
                        if b64_val:
                            try:
                                file_bytes = base64.b64decode(b64_val)
                                st.download_button(label=f"Descargar {key_b64}", data=file_bytes, file_name=f"sct_{key_b64}{ext}", mime=mime, key=f"download_{key_b64}_sct_ind")
                            except Exception:
                                pass
                st.session_state["sct_last_response"] = data_sct
                st.session_state["sct_last_cuit_repr"] = sct_cuit_repr.strip()
        last_sct_data = st.session_state.get("sct_last_response")
        last_sct_cuit = st.session_state.get("sct_last_cuit_repr", "").strip()
        if last_sct_data is not None and last_sct_cuit:
            if st.button("Generar ZIP con archivos MinIO", key="btn_sct_zip_ind"):
                with st.spinner("Descargando archivos desde MinIO..."):
                    zip_bytes, log_df = download_minio_to_zip_by_contributor(
                        [{"cuit_representado": last_sct_cuit, "data": json.dumps(last_sct_data, ensure_ascii=False)}],
                        url_field="data", contributor_field="cuit_representado"
                    )
                if len(log_df) > 0:
                    st.success(f"ZIP generado: {len(log_df)} operaciones")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.download_button(label="Descargar ZIP", data=zip_bytes, file_name=f"sct_{last_sct_cuit}_{date.today().strftime('%Y%m%d')}.zip", mime="application/zip", key="download_sct_zip_ind")
                    with col2:
                        log_xlsx = make_output_excel(log_df, sheet_name="Log")
                        st.download_button(label="Log", data=log_xlsx, file_name=f"log_sct_{last_sct_cuit}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_sct_log_ind")
                else:
                    st.info("No se encontraron URLs de MinIO en la respuesta.")
    else:
        render_minio_mass_download(section_title="SCT", uploader_key="sct_minio_file_upload", timeout_key="sct_minio_timeout", button_key="btn_sct_minio_zip")

# ====================================================================
# TAB: CCMA
# ====================================================================
with tab_ccma:
    st.subheader("CCMA")
    st.markdown("### Cuenta Corriente de Monotributistas y Autonomos (CCMA)")
    st.write("Consulta la cuenta corriente de uno o varios contribuyentes.")
    ccma_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="ccma_mode")
    col_ccma_flags = st.columns(2)
    with col_ccma_flags[0]:
        ccma_proxy = st.checkbox("Usar proxy_request", value=False, key="ccma_proxy_option")
    with col_ccma_flags[1]:
        ccma_movimientos = st.checkbox("Solicitar movimientos", value=True, key="ccma_movimientos_option")
    if ccma_mode == "Individual":
        ccma_cuit_rep = st.text_input("CUIT del representante", value="", key="ccma_cuit_rep_ind")
        ccma_clave_rep = st.text_input("Clave fiscal del representante", value="", type="password", key="ccma_clave_rep_ind")
        ccma_cuit_repr = st.text_input("CUIT del representado", value="", key="ccma_cuit_repr_ind")
        if st.button("Consultar CCMA", key="btn_ccma_consulta_ind"):
            if not (ccma_cuit_rep.strip() and ccma_clave_rep.strip() and ccma_cuit_repr.strip()):
                st.warning("Completa todos los campos obligatorios.")
            else:
                headers_local = build_headers(x_api_key, header_email)
                payload_ccma = {
                    "cuit_representante": ccma_cuit_rep.strip(),
                    "clave_representante": ccma_clave_rep,
                    "cuit_representado": ccma_cuit_repr.strip(),
                    "proxy_request": bool(ccma_proxy), "movimientos": bool(ccma_movimientos)
                }
                with st.spinner("Consultando CCMA..."):
                    resp_ccma = call_ccma_consulta(base_url, headers_local, payload_ccma)
                st.info(f"HTTP status: {resp_ccma.get('http_status')}")
                data_ccma = resp_ccma.get("data")
                st.json(data_ccma)
                resumen_row, movimientos_rows = normalize_ccma_response(resp_ccma.get("http_status"), data_ccma, ccma_cuit_rep.strip(), ccma_cuit_repr.strip(), ccma_movimientos)
                resumen_df, movimientos_df = build_ccma_outputs([resumen_row], movimientos_rows, ccma_movimientos)
                st.write("### Resultado formateado CCMA (vista previa)")
                st.dataframe(resumen_df, use_container_width=True)
                if ccma_movimientos or not movimientos_df.empty:
                    st.write("### Movimientos formateados (vista previa)")
                    st.dataframe(movimientos_df.head(50), use_container_width=True)
                excel_ccma = build_ccma_excel(resumen_df, movimientos_df, include_movements_sheet=ccma_movimientos)
                st.download_button(label="Descargar Excel CCMA", data=excel_ccma, file_name=f"ccma_{date.today().strftime('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_ccma_ind")
                st.session_state["ccma_last_response_json"] = resumen_row.get("response_json", "{}")
                st.session_state["ccma_last_cuit_repr"] = ccma_cuit_repr.strip()
        last_ccma_json = st.session_state.get("ccma_last_response_json")
        last_ccma_cuit = st.session_state.get("ccma_last_cuit_repr", "").strip()
        if last_ccma_json is not None and last_ccma_cuit:
            if st.button("Generar ZIP con archivos MinIO", key="btn_ccma_zip_ind"):
                with st.spinner("Descargando archivos desde MinIO..."):
                    zip_bytes, log_df = download_minio_to_zip_by_contributor(
                        [{"cuit_representado": last_ccma_cuit, "response_json": last_ccma_json}],
                        url_field="response_json", contributor_field="cuit_representado"
                    )
                if len(log_df) > 0:
                    st.success(f"ZIP generado: {len(log_df)} operaciones")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.download_button(label="Descargar ZIP", data=zip_bytes, file_name=f"ccma_{last_ccma_cuit}_{date.today().strftime('%Y%m%d')}.zip", mime="application/zip", key="download_ccma_zip_ind")
                    with col2:
                        log_xlsx = make_output_excel(log_df, sheet_name="Log")
                        st.download_button(label="Log", data=log_xlsx, file_name=f"log_ccma_{last_ccma_cuit}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_ccma_log_ind")
                else:
                    st.info("No se encontraron URLs de MinIO en la respuesta.")
    else:
        render_minio_mass_download(section_title="CCMA", uploader_key="ccma_minio_file_upload", timeout_key="ccma_minio_timeout", button_key="btn_ccma_minio_zip")

# ====================================================================
# TAB: Mis Retenciones
# ====================================================================
with tab_mis_retenciones:
    st.subheader("Mis Retenciones")
    st.markdown("### Mis Retenciones")
    st.write("Consulta retenciones de AFIP.")
    mr_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="mr_mode", horizontal=True)
    mr_desde = st.date_input("Desde", value=date(date.today().year, 1, 1), format="DD/MM/YYYY", key="mr_desde_date")
    mr_hasta = st.date_input("Hasta", value=date.today(), format="DD/MM/YYYY", key="mr_hasta_date")
    mr_minio = st.checkbox("Carga a MinIO", value=True, key="mr_minio_option")
    mr_proxy = st.checkbox("Usar proxy_request", value=False, key="mr_proxy_option")
    if mr_mode == "Individual":
        mr_cuit_rep = st.text_input("CUIT representante", value="", key="mr_cuit_rep_ind")
        mr_clave = st.text_input("Clave representante", value="", type="password", key="mr_clave_ind")
        mr_cuit_repr = st.text_input("CUIT representado (opcional)", value="", key="mr_cuit_repr_ind")
        mr_denominacion = st.text_input("Denominacion", value="", key="mr_denominacion_ind")
        if st.button("Consultar Mis Retenciones", key="btn_mr_consulta_ind"):
            if not (mr_cuit_rep.strip() and mr_clave.strip() and mr_denominacion.strip()):
                st.warning("Completa CUIT representante, clave y denominacion.")
            else:
                headers_local = build_headers(x_api_key, header_email)
                payload_mr = {
                    "cuit_representante": mr_cuit_rep.strip(), "clave_representante": mr_clave,
                    "cuit_representado": mr_cuit_repr.strip() if mr_cuit_repr.strip() else None,
                    "denominacion": mr_denominacion.strip(),
                    "desde": as_ddmmyyyy(mr_desde), "hasta": as_ddmmyyyy(mr_hasta),
                    "carga_minio": bool(mr_minio), "proxy_request": bool(mr_proxy)
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
            if st.button("Generar ZIP con archivos MinIO", key="btn_mr_zip_ind"):
                with st.spinner("Descargando archivos desde MinIO..."):
                    zip_bytes, log_df = download_minio_to_zip_by_contributor(
                        [{"cuit_representado": last_mr_cuit, "data": json.dumps(last_mr_data, ensure_ascii=False)}],
                        url_field="data", contributor_field="cuit_representado"
                    )
                if len(log_df) > 0:
                    st.success(f"ZIP generado: {len(log_df)} operaciones")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.download_button(label="Descargar ZIP", data=zip_bytes, file_name=f"mis_retenciones_{last_mr_cuit}_{date.today().strftime('%Y%m%d')}.zip", mime="application/zip", key="download_mr_zip_ind")
                    with col2:
                        log_xlsx = make_output_excel(log_df, sheet_name="Log")
                        st.download_button(label="Log", data=log_xlsx, file_name=f"log_mis_retenciones_{last_mr_cuit}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_mr_log_ind")
                else:
                    st.info("No se encontraron URLs de MinIO en la respuesta.")
    else:
        render_minio_mass_download(section_title="Mis_Retenciones", uploader_key="mr_minio_file_upload", timeout_key="mr_minio_timeout", button_key="btn_mr_minio_zip")

# ====================================================================
# TAB: SIFERE
# ====================================================================
with tab_sifere:
    st.subheader("SIFERE")
    st.markdown("### SIFERE - Sistema Federal de Recaudacion")
    st.write("Consulta SIFERE por jurisdiccion.")
    sifere_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="sifere_mode", horizontal=True)
    sifere_periodo = st.text_input("Periodo (ej: 202401)", value="", key="sifere_periodo")
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
                st.warning("Completa CUIT representante, clave, CUIT representado y periodo.")
            else:
                jurisdicciones_list = [j.strip() for j in sifere_jurisdicciones.split(",") if j.strip()]
                headers_local = build_headers(x_api_key, header_email)
                payload_sifere = {
                    "cuit_representante": sifere_cuit_rep.strip(), "clave_representante": sifere_clave,
                    "cuit_representado": sifere_cuit_repr.strip(), "periodo": sifere_periodo.strip(),
                    "representado_nombre": sifere_nombre.strip() if sifere_nombre.strip() else None,
                    "jurisdicciones": jurisdicciones_list,
                    "carga_minio": bool(sifere_minio), "proxy_request": bool(sifere_proxy)
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
            if st.button("Generar ZIP con archivos MinIO", key="btn_sifere_zip_ind"):
                with st.spinner("Descargando archivos desde MinIO..."):
                    zip_bytes, log_df = download_minio_to_zip_by_contributor(
                        [{"cuit_representado": last_sifere_cuit, "data": json.dumps(last_sifere_data, ensure_ascii=False)}],
                        url_field="data", contributor_field="cuit_representado"
                    )
                if len(log_df) > 0:
                    st.success(f"ZIP generado: {len(log_df)} operaciones")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.download_button(label="Descargar ZIP", data=zip_bytes, file_name=f"sifere_{last_sifere_cuit}_{date.today().strftime('%Y%m%d')}.zip", mime="application/zip", key="download_sifere_zip_ind")
                    with col2:
                        log_xlsx = make_output_excel(log_df, sheet_name="Log")
                        st.download_button(label="Log", data=log_xlsx, file_name=f"log_sifere_{last_sifere_cuit}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_sifere_log_ind")
                else:
                    st.info("No se encontraron URLs de MinIO en la respuesta.")
    else:
        render_minio_mass_download(section_title="SIFERE", uploader_key="sifere_minio_file_upload", timeout_key="sifere_minio_timeout", button_key="btn_sifere_minio_zip")

# ====================================================================
# TAB: Declaracion en Linea
# ====================================================================
with tab_declaracion_linea:
    st.subheader("Declaracion en Linea")
    st.markdown("### Declaracion en Linea")
    st.write("Consulta declaraciones juradas presentadas.")
    decl_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="decl_mode", horizontal=True)
    decl_periodo_desde = st.text_input("Periodo desde (ej: 202401)", value="", key="decl_periodo_desde")
    decl_periodo_hasta = st.text_input("Periodo hasta (ej: 202412)", value="", key="decl_periodo_hasta")
    decl_minio = st.checkbox("Carga a MinIO", value=True, key="decl_minio_option")
    decl_proxy = st.checkbox("Usar proxy_request", value=False, key="decl_proxy_option")
    if decl_mode == "Individual":
        decl_cuit_rep = st.text_input("CUIT representante", value="", key="decl_cuit_rep_ind")
        decl_clave = st.text_input("Clave representante", value="", type="password", key="decl_clave_ind")
        decl_cuit_repr = st.text_input("CUIT representado (opcional)", value="", key="decl_cuit_repr_ind")
        decl_nombre = st.text_input("Nombre representado (opcional)", value="", key="decl_nombre_ind")
        if st.button("Consultar Declaracion en Linea", key="btn_decl_consulta_ind"):
            if not (decl_cuit_rep.strip() and decl_clave.strip() and decl_periodo_desde.strip() and decl_periodo_hasta.strip()):
                st.warning("Completa CUIT representante, clave y periodos.")
            else:
                headers_local = build_headers(x_api_key, header_email)
                payload_decl = {
                    "cuit_representante": decl_cuit_rep.strip(), "clave_representante": decl_clave,
                    "cuit_representado": decl_cuit_repr.strip() if decl_cuit_repr.strip() else None,
                    "representado_nombre": decl_nombre.strip() if decl_nombre.strip() else None,
                    "periodo_desde": decl_periodo_desde.strip(), "periodo_hasta": decl_periodo_hasta.strip(),
                    "carga_minio": bool(decl_minio), "proxy_request": bool(decl_proxy)
                }
                with st.spinner("Consultando Declaracion en Linea..."):
                    resp_decl = call_declaracion_en_linea_consulta(base_url, headers_local, payload_decl)
                st.info(f"HTTP status: {resp_decl.get('http_status')}")
                st.json(resp_decl.get("data"))
                cuit_id = decl_cuit_repr.strip() if decl_cuit_repr.strip() else decl_cuit_rep.strip()
                st.session_state["decl_last_response"] = resp_decl.get("data")
                st.session_state["decl_last_cuit_id"] = cuit_id
        last_decl_data = st.session_state.get("decl_last_response")
        last_decl_cuit = st.session_state.get("decl_last_cuit_id", "").strip()
        if last_decl_data is not None and last_decl_cuit:
            if st.button("Generar ZIP con archivos MinIO", key="btn_decl_zip_ind"):
                with st.spinner("Descargando archivos desde MinIO..."):
                    zip_bytes, log_df = download_minio_to_zip_by_contributor(
                        [{"cuit_representado": last_decl_cuit, "data": json.dumps(last_decl_data, ensure_ascii=False)}],
                        url_field="data", contributor_field="cuit_representado"
                    )
                if len(log_df) > 0:
                    st.success(f"ZIP generado: {len(log_df)} operaciones")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.download_button(label="Descargar ZIP", data=zip_bytes, file_name=f"declaracion_linea_{last_decl_cuit}_{date.today().strftime('%Y%m%d')}.zip", mime="application/zip", key="download_decl_zip_ind")
                    with col2:
                        log_xlsx = make_output_excel(log_df, sheet_name="Log")
                        st.download_button(label="Log", data=log_xlsx, file_name=f"log_declaracion_linea_{last_decl_cuit}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_decl_log_ind")
                else:
                    st.info("No se encontraron URLs de MinIO en la respuesta.")
    else:
        st.markdown("#### Consulta masiva Declaracion en Linea")
        st.write("Subi un Excel con: **cuit_representante**, **clave_representante**, **cuit_representado** (opcional), **representado_nombre** (opcional)")
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
                st.success(f"Filas leidas: {len(df_decl)}")
                with st.expander("Vista previa"):
                    st.dataframe(df_decl.head(10), use_container_width=True)
                if st.button("Procesar Declaracion en Linea masivo", key="btn_decl_masivo"):
                    headers_local = build_headers(x_api_key, header_email)
                    out_rows = []
                    progress = st.progress(0)
                    status_ph = st.empty()
                    for idx, row in df_decl.reset_index(drop=True).iterrows():
                        cuit_repr = row.get("cuit_representado", "").strip()
                        nombre = row.get("representado_nombre", "").strip()
                        status_ph.info(f"Procesando {idx+1}/{len(df_decl)} - {row['cuit_representante']}")
                        payload = {
                            "cuit_representante": row["cuit_representante"].strip(),
                            "clave_representante": row["clave_representante"],
                            "cuit_representado": cuit_repr if cuit_repr else None,
                            "representado_nombre": nombre if nombre else None,
                            "periodo_desde": decl_periodo_desde.strip(),
                            "periodo_hasta": decl_periodo_hasta.strip(),
                            "carga_minio": bool(decl_minio), "proxy_request": bool(decl_proxy)
                        }
                        resp = call_declaracion_en_linea_consulta(base_url, headers_local, payload)
                        out_rows.append({"cuit_representante": row["cuit_representante"], "cuit_representado": cuit_repr, "http_status": resp.get("http_status"), "data": json.dumps(resp.get("data"), ensure_ascii=False)})
                        progress.progress(int((idx + 1) / len(df_decl) * 100))
                    status_ph.success("Procesamiento finalizado.")
                    result_decl = pd.DataFrame(out_rows)
                    st.dataframe(result_decl.head(50), use_container_width=True)
                    xlsx_decl = make_output_excel(result_decl, sheet_name="Declaracion_Linea")
                    col_dl1, col_dl2 = st.columns(2)
                    with col_dl1:
                        st.download_button(label="Descargar Excel Declaracion en Linea", data=xlsx_decl, file_name=f"consolidado_declaracion_linea_{date.today().strftime('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_decl_masivo")
                    with col_dl2:
                        if st.button("Generar ZIP con archivos MinIO", key="btn_decl_zip"):
                            with st.spinner("Descargando archivos desde MinIO..."):
                                zip_bytes, log_df = download_minio_to_zip_by_contributor(out_rows, url_field="data", contributor_field="cuit_representado")
                            st.success(f"ZIP generado: {len(log_df)} operaciones")
                            st.download_button(label="Descargar ZIP de archivos", data=zip_bytes, file_name=f"declaracion_linea_archivos_{date.today().strftime('%Y%m%d')}.zip", mime="application/zip", key="download_decl_zip_files")
                            log_xlsx = make_output_excel(log_df, sheet_name="Log_Descargas")
                            st.download_button(label="Descargar Log", data=log_xlsx, file_name=f"log_declaracion_linea_{date.today().strftime('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_decl_log")

# ====================================================================
# TAB: Mis Facilidades
# ====================================================================
with tab_mis_facilidades:
    st.subheader("Mis Facilidades")
    st.markdown("### Mis Facilidades")
    st.write("Consulta planes de facilidades de pago.")
    fac_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="fac_mode", horizontal=True)
    fac_minio = st.checkbox("Carga a MinIO", value=True, key="fac_minio_option")
    fac_proxy = st.checkbox("Usar proxy_request", value=False, key="fac_proxy_option")
    if fac_mode == "Individual":
        fac_cuit_login = st.text_input("CUIT login", value="", key="fac_cuit_login_ind")
        fac_clave = st.text_input("Clave", value="", type="password", key="fac_clave_ind")
        fac_cuit_repr = st.text_input("CUIT representado (opcional)", value="", key="fac_cuit_repr_ind")
        fac_denominacion = st.text_input("Denominacion (opcional)", value="", key="fac_denominacion_ind")
        if st.button("Consultar Mis Facilidades", key="btn_fac_consulta_ind"):
            if not (fac_cuit_login.strip() and fac_clave.strip()):
                st.warning("Completa CUIT login y clave.")
            else:
                headers_local = build_headers(x_api_key, header_email)
                payload_fac = {
                    "cuit_login": fac_cuit_login.strip(), "clave": fac_clave,
                    "cuit_representado": fac_cuit_repr.strip() if fac_cuit_repr.strip() else None,
                    "denominacion": fac_denominacion.strip() if fac_denominacion.strip() else None,
                    "carga_minio": bool(fac_minio), "proxy_request": bool(fac_proxy)
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
            if st.button("Generar ZIP con archivos MinIO", key="btn_fac_zip_ind"):
                with st.spinner("Descargando archivos desde MinIO..."):
                    zip_bytes, log_df = download_minio_to_zip_by_contributor(
                        [{"cuit_representado": last_fac_cuit, "data": json.dumps(last_fac_data, ensure_ascii=False)}],
                        url_field="data", contributor_field="cuit_representado"
                    )
                if len(log_df) > 0:
                    st.success(f"ZIP generado: {len(log_df)} operaciones")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.download_button(label="Descargar ZIP", data=zip_bytes, file_name=f"mis_facilidades_{last_fac_cuit}_{date.today().strftime('%Y%m%d')}.zip", mime="application/zip", key="download_fac_zip_ind")
                    with col2:
                        log_xlsx = make_output_excel(log_df, sheet_name="Log")
                        st.download_button(label="Log", data=log_xlsx, file_name=f"log_mis_facilidades_{last_fac_cuit}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_fac_log_ind")
                else:
                    st.info("No se encontraron URLs de MinIO en la respuesta.")
    else:
        st.markdown("#### Consulta masiva Mis Facilidades")
        st.write("Subi un Excel con: **cuit_login**, **clave**, **cuit_representado** (opcional), **denominacion** (opcional)")
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
                st.success(f"Filas leidas: {len(df_fac)}")
                with st.expander("Vista previa"):
                    st.dataframe(df_fac.head(10), use_container_width=True)
                if st.button("Procesar Mis Facilidades masivo", key="btn_fac_masivo"):
                    headers_local = build_headers(x_api_key, header_email)
                    out_rows = []
                    progress = st.progress(0)
                    status_ph = st.empty()
                    for idx, row in df_fac.reset_index(drop=True).iterrows():
                        cuit_repr = row.get("cuit_representado", "").strip()
                        denom = row.get("denominacion", "").strip()
                        status_ph.info(f"Procesando {idx+1}/{len(df_fac)} - {row['cuit_login']}")
                        payload = {
                            "cuit_login": row["cuit_login"].strip(), "clave": row["clave"],
                            "cuit_representado": cuit_repr if cuit_repr else None,
                            "denominacion": denom if denom else None,
                            "carga_minio": bool(fac_minio), "proxy_request": bool(fac_proxy)
                        }
                        resp = call_mis_facilidades_consulta(base_url, headers_local, payload)
                        out_rows.append({"cuit_login": row["cuit_login"], "cuit_representado": cuit_repr, "http_status": resp.get("http_status"), "data": json.dumps(resp.get("data"), ensure_ascii=False)})
                        progress.progress(int((idx + 1) / len(df_fac) * 100))
                    status_ph.success("Procesamiento finalizado.")
                    result_fac = pd.DataFrame(out_rows)
                    st.dataframe(result_fac.head(50), use_container_width=True)
                    xlsx_fac = make_output_excel(result_fac, sheet_name="Mis_Facilidades")
                    col_dl1, col_dl2 = st.columns(2)
                    with col_dl1:
                        st.download_button(label="Descargar Excel Mis Facilidades", data=xlsx_fac, file_name=f"consolidado_mis_facilidades_{date.today().strftime('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_fac_masivo")
                    with col_dl2:
                        if st.button("Generar ZIP con archivos MinIO", key="btn_fac_zip"):
                            with st.spinner("Descargando archivos desde MinIO..."):
                                zip_bytes, log_df = download_minio_to_zip_by_contributor(out_rows, url_field="data", contributor_field="cuit_representado")
                            st.success(f"ZIP generado: {len(log_df)} operaciones")
                            st.download_button(label="Descargar ZIP de archivos", data=zip_bytes, file_name=f"mis_facilidades_archivos_{date.today().strftime('%Y%m%d')}.zip", mime="application/zip", key="download_fac_zip_files")
                            log_xlsx = make_output_excel(log_df, sheet_name="Log_Descargas")
                            st.download_button(label="Descargar Log", data=log_xlsx, file_name=f"log_mis_facilidades_{date.today().strftime('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_fac_log")

# ====================================================================
# TAB: Aportes en Linea
# ====================================================================
with tab_aportes_linea:
    st.subheader("Aportes en Linea")
    st.markdown("### Aportes en Linea")
    st.write("Consulta aportes y contribuciones en linea.")
    ap_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="ap_mode", horizontal=True)
    ap_minio = st.checkbox("Archivo historico MinIO", value=True, key="ap_minio_option")
    ap_b64 = st.checkbox("Archivo historico base64", value=False, key="ap_b64_option")
    ap_proxy = st.checkbox("Usar proxy_request", value=False, key="ap_proxy_option")
    if ap_mode == "Individual":
        ap_cuit_login = st.text_input("CUIT login", value="", key="ap_cuit_login_ind")
        ap_clave = st.text_input("Clave", value="", type="password", key="ap_clave_ind")
        ap_cuit_repr = st.text_input("CUIT representado (opcional)", value="", key="ap_cuit_repr_ind")
        if st.button("Consultar Aportes en Linea", key="btn_ap_consulta_ind"):
            if not (ap_cuit_login.strip() and ap_clave.strip()):
                st.warning("Completa CUIT login y clave.")
            else:
                headers_local = build_headers(x_api_key, header_email)
                payload_ap = {
                    "cuit_login": ap_cuit_login.strip(), "clave": ap_clave,
                    "cuit_representado": ap_cuit_repr.strip() if ap_cuit_repr.strip() else None,
                    "archivo_historico_b64": bool(ap_b64),
                    "archivo_historico_minio": bool(ap_minio),
                    "proxy_request": bool(ap_proxy)
                }
                with st.spinner("Consultando Aportes en Linea..."):
                    resp_ap = call_aportes_en_linea_consulta(base_url, headers_local, payload_ap)
                st.info(f"HTTP status: {resp_ap.get('http_status')}")
                st.json(resp_ap.get("data"))
                cuit_id = ap_cuit_repr.strip() if ap_cuit_repr.strip() else ap_cuit_login.strip()
                st.session_state["ap_last_response"] = resp_ap.get("data")
                st.session_state["ap_last_cuit_id"] = cuit_id
        last_ap_data = st.session_state.get("ap_last_response")
        last_ap_cuit = st.session_state.get("ap_last_cuit_id", "").strip()
        if last_ap_data is not None and last_ap_cuit:
            if st.button("Generar ZIP con archivos MinIO", key="btn_ap_zip_ind"):
                with st.spinner("Descargando archivos desde MinIO..."):
                    zip_bytes, log_df = download_minio_to_zip_by_contributor(
                        [{"cuit_representado": last_ap_cuit, "data": json.dumps(last_ap_data, ensure_ascii=False)}],
                        url_field="data", contributor_field="cuit_representado"
                    )
                if len(log_df) > 0:
                    st.success(f"ZIP generado: {len(log_df)} operaciones")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.download_button(label="Descargar ZIP", data=zip_bytes, file_name=f"aportes_linea_{last_ap_cuit}_{date.today().strftime('%Y%m%d')}.zip", mime="application/zip", key="download_ap_zip_ind")
                    with col2:
                        log_xlsx = make_output_excel(log_df, sheet_name="Log")
                        st.download_button(label="Log", data=log_xlsx, file_name=f"log_aportes_linea_{last_ap_cuit}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_ap_log_ind")
                else:
                    st.info("No se encontraron URLs de MinIO en la respuesta.")
    else:
        st.markdown("#### Consulta masiva Aportes en Linea")
        st.write("Subi un Excel con: **cuit_login**, **clave**, **cuit_representado** (opcional)")
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
                st.success(f"Filas leidas: {len(df_ap)}")
                with st.expander("Vista previa"):
                    st.dataframe(df_ap.head(10), use_container_width=True)
                if st.button("Procesar Aportes en Linea masivo", key="btn_ap_masivo"):
                    headers_local = build_headers(x_api_key, header_email)
                    out_rows = []
                    progress = st.progress(0)
                    status_ph = st.empty()
                    for idx, row in df_ap.reset_index(drop=True).iterrows():
                        cuit_repr = row.get("cuit_representado", "").strip()
                        status_ph.info(f"Procesando {idx+1}/{len(df_ap)} - {row['cuit_login']}")
                        payload = {
                            "cuit_login": row["cuit_login"].strip(), "clave": row["clave"],
                            "cuit_representado": cuit_repr if cuit_repr else None,
                            "archivo_historico_b64": bool(ap_b64),
                            "archivo_historico_minio": bool(ap_minio),
                            "proxy_request": bool(ap_proxy)
                        }
                        resp = call_aportes_en_linea_consulta(base_url, headers_local, payload)
                        out_rows.append({"cuit_login": row["cuit_login"], "cuit_representado": cuit_repr, "http_status": resp.get("http_status"), "data": json.dumps(resp.get("data"), ensure_ascii=False)})
                        progress.progress(int((idx + 1) / len(df_ap) * 100))
                    status_ph.success("Procesamiento finalizado.")
                    result_ap = pd.DataFrame(out_rows)
                    st.dataframe(result_ap.head(50), use_container_width=True)
                    xlsx_ap = make_output_excel(result_ap, sheet_name="Aportes_Linea")
                    col_dl1, col_dl2 = st.columns(2)
                    with col_dl1:
                        st.download_button(label="Descargar Excel Aportes en Linea", data=xlsx_ap, file_name=f"consolidado_aportes_linea_{date.today().strftime('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_ap_masivo")
                    with col_dl2:
                        if st.button("Generar ZIP con archivos MinIO", key="btn_ap_zip"):
                            with st.spinner("Descargando archivos desde MinIO..."):
                                zip_bytes, log_df = download_minio_to_zip_by_contributor(out_rows, url_field="data", contributor_field="cuit_representado")
                            st.success(f"ZIP generado: {len(log_df)} operaciones")
                            st.download_button(label="Descargar ZIP de archivos", data=zip_bytes, file_name=f"aportes_linea_archivos_{date.today().strftime('%Y%m%d')}.zip", mime="application/zip", key="download_ap_zip_files")
                            log_xlsx = make_output_excel(log_df, sheet_name="Log_Descargas")
                            st.download_button(label="Descargar Log", data=log_xlsx, file_name=f"log_aportes_linea_{date.today().strftime('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_ap_log")

# ====================================================================
# TAB: APOC
# ====================================================================
with tab_apoc:
    st.subheader("APOC")
    st.markdown("### Consulta de Apocrifos")
    st.write("Verifica si uno o varios CUITs se encuentran en la base de apocrifos.")
    apoc_mode = st.radio("Tipo de consulta", ["Individual", "Masiva"], key="apoc_mode", horizontal=True)
    if apoc_mode == "Individual":
        apoc_cuit = st.text_input("CUIT a consultar", value="", key="apoc_cuit_individual")
        if st.button("Consultar Apocrifo individual", key="btn_apoc_consulta_ind"):
            if not apoc_cuit.strip():
                st.warning("Ingresa un CUIT para consultar.")
            else:
                headers_local = build_headers(x_api_key, header_email)
                with st.spinner("Consultando apocrifo individual..."):
                    resp_apoc = call_apoc_consulta(base_url, headers_local, apoc_cuit.strip())
                st.info(f"HTTP status: {resp_apoc.get('http_status')}")
                st.json(resp_apoc.get("data"))
    else:
        cuits_text_apoc = st.text_area("Lista de CUITs (separados por comas, espacios o saltos de linea)", value="", height=150, key="apoc_cuits_masivo")
        if st.button("Consultar Apocrifos masivos", key="btn_apoc_consulta_masivo"):
            raw = cuits_text_apoc.replace("\n", ",")
            cuits_list = [c.strip() for c in re.split(r",|\s", raw) if c.strip()]
            if not cuits_list:
                st.warning("Ingresa al menos un CUIT para la consulta masiva.")
            else:
                headers_local = build_headers(x_api_key, header_email)
                out_rows_apoc: List[Dict[str, Any]] = []
                progress = st.progress(0)
                with st.spinner("Consultando apocrifos masivos..."):
                    for idx, cuit in enumerate(cuits_list):
                        resp = call_apoc_consulta(base_url, headers_local, cuit)
                        http_status = resp.get("http_status")
                        data = resp.get("data")
                        es_apoc = None; fecha_apoc = None; fecha_publicacion = None
                        if isinstance(data, dict):
                            es_apoc = data.get("apoc") or data.get("es_apocrifo")
                            fecha_apoc = data.get("fecha_apoc") or data.get("fecha")
                            fecha_publicacion = data.get("fecha_publicacion")
                        out_rows_apoc.append({"cuit": cuit, "http_status": http_status, "apoc": es_apoc, "fecha_apoc": fecha_apoc, "fecha_publicacion": fecha_publicacion, "data": json.dumps(data, ensure_ascii=False)})
                        progress.progress(int((idx + 1) / len(cuits_list) * 100))
                df_apoc = pd.DataFrame(out_rows_apoc)
                st.write("### Resultado de consultas de Apocrifos (vista previa)")
                st.dataframe(df_apoc.head(50), use_container_width=True)
                xlsx_bytes_apoc = make_output_excel(df_apoc, sheet_name="Apoc_Masivo")
                st.download_button(label="Descargar Excel de resultados Apocrifos", data=xlsx_bytes_apoc, file_name=f"consolidado_apoc_{date.today().strftime('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_apoc_masivo")

# ====================================================================
# TAB: Consulta de CUIT
# ====================================================================
with tab_cuit:
    st.subheader("Consulta de Constancia de CUIT")
    st.markdown("### Consulta de CUIT")
    st.write("Obten la constancia de inscripcion de uno o varios CUITs.")
    mode = st.radio("Tipo de consulta", ["Individual", "Masiva"], key="cuit_mode", horizontal=True)
    if mode == "Individual":
        cuit_individual = st.text_input("CUIT individual", value="", key="cuit_individual")
        if st.button("Consultar CUIT individual", key="btn_cuit_individual"):
            if not cuit_individual.strip():
                st.warning("Ingresa el CUIT a consultar.")
            else:
                headers_local = build_headers(x_api_key, header_email)
                payload_cuit_ind = {"cuit": cuit_individual.strip()}
                with st.spinner("Consultando CUIT individual..."):
                    resp_cuit_ind = call_cuit_individual(base_url, headers_local, payload_cuit_ind)
                st.info(f"HTTP status: {resp_cuit_ind.get('http_status')}")
                st.json(resp_cuit_ind.get("data"))
    else:
        cuits_text = st.text_area("Lista de CUITs (separados por comas, espacios o saltos de linea)", value="", height=150, key="cuits_masivo")
        if st.button("Consultar CUITs masivos", key="btn_cuit_masivo"):
            raw = cuits_text.replace("\n", ",")
            cuits_list = [c.strip() for c in re.split(r",|\s", raw) if c.strip()]
            if not cuits_list:
                st.warning("Ingresa al menos un CUIT para la consulta masiva.")
            else:
                headers_local = build_headers(x_api_key, header_email)
                payload_cuit_mass = {"cuits": cuits_list}
                with st.spinner("Consultando CUITs masivos..."):
                    resp_cuit_mass = call_cuit_masivo(base_url, headers_local, payload_cuit_mass)
                st.info(f"HTTP status: {resp_cuit_mass.get('http_status')}")
                st.json(resp_cuit_mass.get("data"))

# ========================================================================
# === NUEVAS TABS (Fases 1-3) ============================================
# ========================================================================

# ====================================================================
# TAB: Pago y Devoluciones
# ====================================================================
with tab_pago_devoluciones:
    st.subheader("Pago y Devoluciones")
    st.markdown("### Pago y Devoluciones")
    st.write("Consulta pagos y devoluciones de AFIP.")
    pd_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="pd_mode", horizontal=True)
    pd_minio = st.checkbox("Carga a MinIO", value=True, key="pd_minio_option")
    pd_proxy = st.checkbox("Usar proxy_request", value=False, key="pd_proxy_option")
    if pd_mode == "Individual":
        pd_cuit_rep = st.text_input("CUIT representante", value="", key="pd_cuit_rep_ind")
        pd_clave = st.text_input("Clave representante", value="", type="password", key="pd_clave_ind")
        pd_cuit_repr = st.text_input("CUIT representado (opcional)", value="", key="pd_cuit_repr_ind")
        if st.button("Consultar Pago y Devoluciones", key="btn_pd_consulta_ind"):
            if not (pd_cuit_rep.strip() and pd_clave.strip()):
                st.warning("Completa CUIT representante y clave.")
            else:
                headers_local = build_headers(x_api_key, header_email)
                payload_pd = {
                    "cuit_representante": pd_cuit_rep.strip(),
                    "clave_representante": pd_clave,
                    "cuit_representado": pd_cuit_repr.strip() if pd_cuit_repr.strip() else None,
                    "carga_minio": bool(pd_minio), "proxy_request": bool(pd_proxy)
                }
                with st.spinner("Consultando Pago y Devoluciones..."):
                    resp_pd = call_pago_devoluciones_consulta(base_url, headers_local, payload_pd)
                st.info(f"HTTP status: {resp_pd.get('http_status')}")
                st.json(resp_pd.get("data"))
                cuit_id = pd_cuit_repr.strip() if pd_cuit_repr.strip() else pd_cuit_rep.strip()
                st.session_state["pd_last_response"] = resp_pd.get("data")
                st.session_state["pd_last_cuit_id"] = cuit_id
        last_pd_data = st.session_state.get("pd_last_response")
        last_pd_cuit = st.session_state.get("pd_last_cuit_id", "").strip()
        if last_pd_data is not None and last_pd_cuit:
            if st.button("Generar ZIP con archivos MinIO", key="btn_pd_zip_ind"):
                with st.spinner("Descargando archivos desde MinIO..."):
                    zip_bytes, log_df = download_minio_to_zip_by_contributor(
                        [{"cuit_representado": last_pd_cuit, "data": json.dumps(last_pd_data, ensure_ascii=False)}],
                        url_field="data", contributor_field="cuit_representado"
                    )
                if len(log_df) > 0:
                    st.success(f"ZIP generado: {len(log_df)} operaciones")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.download_button(label="Descargar ZIP", data=zip_bytes, file_name=f"pago_devoluciones_{last_pd_cuit}_{date.today().strftime('%Y%m%d')}.zip", mime="application/zip", key="download_pd_zip_ind")
                    with col2:
                        log_xlsx = make_output_excel(log_df, sheet_name="Log")
                        st.download_button(label="Log", data=log_xlsx, file_name=f"log_pd_{last_pd_cuit}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_pd_log_ind")
                else:
                    st.info("No se encontraron URLs de MinIO en la respuesta.")
    else:
        render_minio_mass_download(section_title="Pago_y_Devoluciones", uploader_key="pd_minio_file_upload", timeout_key="pd_minio_timeout", button_key="btn_pd_minio_zip")

# ====================================================================
# TAB: Hacienda
# ====================================================================
with tab_hacienda:
    st.subheader("Hacienda")
    st.markdown("### Hacienda")
    st.write("Consulta comprobantes de Hacienda (AFIP).")
    ha_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="ha_mode", horizontal=True)
    ha_desde = st.date_input("Desde", value=date(date.today().year, 1, 1), format="DD/MM/YYYY", key="ha_desde_date")
    ha_hasta = st.date_input("Hasta", value=date.today(), format="DD/MM/YYYY", key="ha_hasta_date")
    ha_minio = st.checkbox("Carga a MinIO", value=True, key="ha_minio_option")
    ha_proxy = st.checkbox("Usar proxy_request", value=False, key="ha_proxy_option")
    if ha_mode == "Individual":
        ha_cuit_rep = st.text_input("CUIT representante", value="", key="ha_cuit_rep_ind")
        ha_clave = st.text_input("Clave", value="", type="password", key="ha_clave_ind")
        ha_cuit_repr = st.text_input("CUIT representado", value="", key="ha_cuit_repr_ind")
        ha_denominacion = st.text_input("Denominacion", value="", key="ha_denominacion_ind")
        if st.button("Consultar Hacienda", key="btn_ha_consulta_ind"):
            if not (ha_cuit_rep.strip() and ha_clave.strip() and ha_cuit_repr.strip() and ha_denominacion.strip()):
                st.warning("Completa todos los campos obligatorios.")
            else:
                headers_local = build_headers(x_api_key, header_email)
                payload_ha = {
                    "desde": as_ddmmyyyy(ha_desde), "hasta": as_ddmmyyyy(ha_hasta),
                    "cuit_representante": ha_cuit_rep.strip(), "denominacion": ha_denominacion.strip(),
                    "representado_cuit": ha_cuit_repr.strip(), "clave": ha_clave,
                    "minio_upload": bool(ha_minio), "proxy_request": bool(ha_proxy)
                }
                with st.spinner("Consultando Hacienda..."):
                    resp_ha = call_hacienda_consulta(base_url, headers_local, payload_ha)
                st.info(f"HTTP status: {resp_ha.get('http_status')}")
                st.json(resp_ha.get("data"))
                st.session_state["ha_last_response"] = resp_ha.get("data")
                st.session_state["ha_last_cuit_repr"] = ha_cuit_repr.strip()
        last_ha_data = st.session_state.get("ha_last_response")
        last_ha_cuit = st.session_state.get("ha_last_cuit_repr", "").strip()
        if last_ha_data is not None and last_ha_cuit:
            if st.button("Generar ZIP con archivos MinIO", key="btn_ha_zip_ind"):
                with st.spinner("Descargando archivos desde MinIO..."):
                    zip_bytes, log_df = download_minio_to_zip_by_contributor(
                        [{"cuit_representado": last_ha_cuit, "data": json.dumps(last_ha_data, ensure_ascii=False)}],
                        url_field="data", contributor_field="cuit_representado"
                    )
                if len(log_df) > 0:
                    st.success(f"ZIP generado: {len(log_df)} operaciones")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.download_button(label="Descargar ZIP", data=zip_bytes, file_name=f"hacienda_{last_ha_cuit}_{date.today().strftime('%Y%m%d')}.zip", mime="application/zip", key="download_ha_zip_ind")
                    with col2:
                        log_xlsx = make_output_excel(log_df, sheet_name="Log")
                        st.download_button(label="Log", data=log_xlsx, file_name=f"log_hacienda_{last_ha_cuit}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_ha_log_ind")
                else:
                    st.info("No se encontraron URLs de MinIO en la respuesta.")
    else:
        render_minio_mass_download(section_title="Hacienda", uploader_key="ha_minio_file_upload", timeout_key="ha_minio_timeout", button_key="btn_ha_minio_zip")

# ====================================================================
# TAB: Liquidacion Granos
# ====================================================================
with tab_liquidacion_granos:
    st.subheader("Liquidacion Granos")
    st.markdown("### Liquidacion Granos")
    st.write("Consulta liquidaciones de granos (AFIP).")
    lg_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="lg_mode", horizontal=True)
    lg_desde = st.date_input("Desde", value=date(date.today().year, 1, 1), format="DD/MM/YYYY", key="lg_desde_date")
    lg_hasta = st.date_input("Hasta", value=date.today(), format="DD/MM/YYYY", key="lg_hasta_date")
    lg_minio = st.checkbox("Carga a MinIO", value=True, key="lg_minio_option")
    lg_proxy = st.checkbox("Usar proxy_request", value=False, key="lg_proxy_option")
    if lg_mode == "Individual":
        lg_cuit_rep = st.text_input("CUIT representante", value="", key="lg_cuit_rep_ind")
        lg_clave = st.text_input("Clave", value="", type="password", key="lg_clave_ind")
        lg_cuit_repr = st.text_input("CUIT representado (opcional)", value="", key="lg_cuit_repr_ind")
        lg_denominacion = st.text_input("Denominacion", value="", key="lg_denominacion_ind")
        if st.button("Consultar Liquidacion Granos", key="btn_lg_consulta_ind"):
            if not (lg_cuit_rep.strip() and lg_clave.strip() and lg_denominacion.strip()):
                st.warning("Completa CUIT representante, clave y denominacion.")
            else:
                headers_local = build_headers(x_api_key, header_email)
                payload_lg = {
                    "desde": as_ddmmyyyy(lg_desde), "hasta": as_ddmmyyyy(lg_hasta),
                    "cuit_representante": lg_cuit_rep.strip(), "clave": lg_clave,
                    "denominacion": lg_denominacion.strip(),
                    "cuit_representado": lg_cuit_repr.strip() if lg_cuit_repr.strip() else None,
                    "minio_upload": bool(lg_minio), "proxy_request": bool(lg_proxy)
                }
                with st.spinner("Consultando Liquidacion Granos..."):
                    resp_lg = call_liquidacion_granos_consulta(base_url, headers_local, payload_lg)
                st.info(f"HTTP status: {resp_lg.get('http_status')}")
                st.json(resp_lg.get("data"))
                cuit_id = lg_cuit_repr.strip() if lg_cuit_repr.strip() else lg_cuit_rep.strip()
                st.session_state["lg_last_response"] = resp_lg.get("data")
                st.session_state["lg_last_cuit_id"] = cuit_id
        last_lg_data = st.session_state.get("lg_last_response")
        last_lg_cuit = st.session_state.get("lg_last_cuit_id", "").strip()
        if last_lg_data is not None and last_lg_cuit:
            if st.button("Generar ZIP con archivos MinIO", key="btn_lg_zip_ind"):
                with st.spinner("Descargando archivos desde MinIO..."):
                    zip_bytes, log_df = download_minio_to_zip_by_contributor(
                        [{"cuit_representado": last_lg_cuit, "data": json.dumps(last_lg_data, ensure_ascii=False)}],
                        url_field="data", contributor_field="cuit_representado"
                    )
                if len(log_df) > 0:
                    st.success(f"ZIP generado: {len(log_df)} operaciones")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.download_button(label="Descargar ZIP", data=zip_bytes, file_name=f"liquidacion_granos_{last_lg_cuit}_{date.today().strftime('%Y%m%d')}.zip", mime="application/zip", key="download_lg_zip_ind")
                    with col2:
                        log_xlsx = make_output_excel(log_df, sheet_name="Log")
                        st.download_button(label="Log", data=log_xlsx, file_name=f"log_lg_{last_lg_cuit}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_lg_log_ind")
                else:
                    st.info("No se encontraron URLs de MinIO en la respuesta.")
    else:
        render_minio_mass_download(section_title="Liquidacion_Granos", uploader_key="lg_minio_file_upload", timeout_key="lg_minio_timeout", button_key="btn_lg_minio_zip")

# ====================================================================
# TAB: Portal IVA
# ====================================================================
with tab_portal_iva:
    st.subheader("Portal IVA")
    st.markdown("### Portal IVA")
    st.write("Consulta el Portal IVA de AFIP con multiples opciones de configuracion.")
    piva_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="piva_mode", horizontal=True)
    piva_periodo = st.text_input("Periodo (ej: 202401)", value="", key="piva_periodo")
    piva_minio = st.checkbox("Carga a MinIO", value=True, key="piva_minio_option")
    piva_proxy = st.checkbox("Usar proxy_request", value=False, key="piva_proxy_option")
    with st.expander("Opciones de consulta Portal IVA", expanded=False):
        col_piva1, col_piva2, col_piva3 = st.columns(3)
        with col_piva1:
            piva_operaciones = st.checkbox("Operaciones NG o E", value=False, key="piva_operaciones")
            piva_prorrateo_global = st.checkbox("Prorrateo global", value=False, key="piva_prorrateo_global")
            piva_prorrateo_directa = st.checkbox("Prorrateo asig. directa", value=False, key="piva_prorrateo_directa")
        with col_piva2:
            piva_prorrateo_ambos = st.checkbox("Prorrateo ambos", value=False, key="piva_prorrateo_ambos")
            piva_importacion_bienes = st.checkbox("Importacion def. bienes", value=False, key="piva_importacion_bienes")
            piva_importacion_servicios = st.checkbox("Importacion servicios", value=False, key="piva_importacion_servicios")
        with col_piva3:
            piva_turiva = st.checkbox("Regimen TURIVA", value=False, key="piva_turiva")
            piva_bienes_usados = st.checkbox("Bienes usados", value=False, key="piva_bienes_usados")
            piva_ninguna = st.checkbox("Ninguna de las anteriores", value=True, key="piva_ninguna")
        piva_csv_ventas = st.checkbox("Descargar CSV Ventas", value=True, key="piva_csv_ventas")
        piva_csv_compras = st.checkbox("Descargar CSV Compras", value=True, key="piva_csv_compras")
    if piva_mode == "Individual":
        piva_cuit_rep = st.text_input("CUIT representante", value="", key="piva_cuit_rep_ind")
        piva_clave = st.text_input("Clave representante", value="", type="password", key="piva_clave_ind")
        piva_cuit_repr = st.text_input("CUIT representado", value="", key="piva_cuit_repr_ind")
        piva_denominacion = st.text_input("Denominacion", value="", key="piva_denominacion_ind")
        if st.button("Consultar Portal IVA", key="btn_piva_consulta_ind"):
            if not (piva_cuit_rep.strip() and piva_clave.strip() and piva_cuit_repr.strip() and piva_periodo.strip()):
                st.warning("Completa CUIT representante, clave, CUIT representado y periodo.")
            else:
                headers_local = build_headers(x_api_key, header_email)
                payload_piva = {
                    "cuit_representante": piva_cuit_rep.strip(),
                    "clave_representante": piva_clave,
                    "cuit_representado": piva_cuit_repr.strip(),
                    "denominacion": piva_denominacion.strip(),
                    "periodo": piva_periodo.strip(),
                    "operaciones_ng_o_e": bool(piva_operaciones),
                    "prorrateo_global": bool(piva_prorrateo_global),
                    "prorrateo_asignacion_directa": bool(piva_prorrateo_directa),
                    "prorrateo_ambos": bool(piva_prorrateo_ambos),
                    "importacion_definitiva_bienes": bool(piva_importacion_bienes),
                    "importacion_servicios": bool(piva_importacion_servicios),
                    "regimen_turiva": bool(piva_turiva),
                    "bienes_usados": bool(piva_bienes_usados),
                    "ninguna_anteriores": bool(piva_ninguna),
                    "descarga_csv_ventas": bool(piva_csv_ventas),
                    "descarga_csv_compras": bool(piva_csv_compras),
                    "carga_minio": bool(piva_minio),
                    "proxy_request": bool(piva_proxy)
                }
                with st.spinner("Consultando Portal IVA..."):
                    resp_piva = call_portal_iva_consulta(base_url, headers_local, payload_piva)
                st.info(f"HTTP status: {resp_piva.get('http_status')}")
                st.json(resp_piva.get("data"))
                st.session_state["piva_last_response"] = resp_piva.get("data")
                st.session_state["piva_last_cuit_repr"] = piva_cuit_repr.strip()
        last_piva_data = st.session_state.get("piva_last_response")
        last_piva_cuit = st.session_state.get("piva_last_cuit_repr", "").strip()
        if last_piva_data is not None and last_piva_cuit:
            if st.button("Generar ZIP con archivos MinIO", key="btn_piva_zip_ind"):
                with st.spinner("Descargando archivos desde MinIO..."):
                    zip_bytes, log_df = download_minio_to_zip_by_contributor(
                        [{"cuit_representado": last_piva_cuit, "data": json.dumps(last_piva_data, ensure_ascii=False)}],
                        url_field="data", contributor_field="cuit_representado"
                    )
                if len(log_df) > 0:
                    st.success(f"ZIP generado: {len(log_df)} operaciones")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.download_button(label="Descargar ZIP", data=zip_bytes, file_name=f"portal_iva_{last_piva_cuit}_{date.today().strftime('%Y%m%d')}.zip", mime="application/zip", key="download_piva_zip_ind")
                    with col2:
                        log_xlsx = make_output_excel(log_df, sheet_name="Log")
                        st.download_button(label="Log", data=log_xlsx, file_name=f"log_portal_iva_{last_piva_cuit}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_piva_log_ind")
                else:
                    st.info("No se encontraron URLs de MinIO en la respuesta.")
    else:
        render_minio_mass_download(section_title="Portal_IVA", uploader_key="piva_minio_file_upload", timeout_key="piva_minio_timeout", button_key="btn_piva_minio_zip")

# ====================================================================
# TAB: Retenciones / Percepciones Provinciales (ARBA / AGIP / Misiones)
# ====================================================================
with tab_ret_provinciales:
    st.subheader("Retenciones y Percepciones Provinciales")
    subtab_arba, subtab_agip, subtab_misiones = st.tabs(["ARBA", "AGIP", "Misiones"])

    # --- ARBA ---
    with subtab_arba:
        st.markdown("#### ARBA - Retenciones y Percepciones IIBB")
        arba_mode = st.radio("Modo", ["Individual", "Masiva"], key="arba_mode", horizontal=True)
        arba_minio = st.checkbox("Carga a MinIO", value=True, key="arba_minio_option")
        arba_proxy = st.checkbox("Usar proxy_request", value=False, key="arba_proxy_option")
        if arba_mode == "Individual":
            arba_cuit = st.text_input("CUIT", value="", key="arba_cuit_ind")
            arba_clave = st.text_input("Clave", value="", type="password", key="arba_clave_ind")
            arba_periodo = st.text_input("Periodo (ej: 202401)", value="", key="arba_periodo_ind")
            arba_denominacion = st.text_input("Denominacion", value="", key="arba_denominacion_ind")
            if st.button("Consultar ARBA", key="btn_arba_consulta_ind"):
                if not (arba_cuit.strip() and arba_clave.strip() and arba_periodo.strip()):
                    st.warning("Completa CUIT, clave y periodo.")
                else:
                    headers_local = build_headers(x_api_key, header_email)
                    payload_arba = {
                        "cuit": arba_cuit.strip(), "clave": arba_clave,
                        "periodo": arba_periodo.strip(), "denominacion": arba_denominacion.strip(),
                        "carga_minio": bool(arba_minio), "proxy_request": bool(arba_proxy)
                    }
                    with st.spinner("Consultando ARBA..."):
                        resp_arba = call_arba_consulta(base_url, headers_local, payload_arba)
                    st.info(f"HTTP status: {resp_arba.get('http_status')}")
                    st.json(resp_arba.get("data"))
                    st.session_state["arba_last_response"] = resp_arba.get("data")
                    st.session_state["arba_last_cuit"] = arba_cuit.strip()
            last_arba_data = st.session_state.get("arba_last_response")
            last_arba_cuit = st.session_state.get("arba_last_cuit", "").strip()
            if last_arba_data is not None and last_arba_cuit:
                if st.button("Generar ZIP con archivos MinIO", key="btn_arba_zip_ind"):
                    with st.spinner("Descargando archivos desde MinIO..."):
                        zip_bytes, log_df = download_minio_to_zip_by_contributor(
                            [{"cuit_representado": last_arba_cuit, "data": json.dumps(last_arba_data, ensure_ascii=False)}],
                            url_field="data", contributor_field="cuit_representado"
                        )
                    if len(log_df) > 0:
                        st.success(f"ZIP generado: {len(log_df)} operaciones")
                        col1, col2 = st.columns(2)
                        with col1:
                            st.download_button(label="Descargar ZIP", data=zip_bytes, file_name=f"arba_{last_arba_cuit}_{date.today().strftime('%Y%m%d')}.zip", mime="application/zip", key="download_arba_zip_ind")
                        with col2:
                            log_xlsx = make_output_excel(log_df, sheet_name="Log")
                            st.download_button(label="Log", data=log_xlsx, file_name=f"log_arba_{last_arba_cuit}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_arba_log_ind")
                    else:
                        st.info("No se encontraron URLs de MinIO en la respuesta.")
        else:
            render_minio_mass_download(section_title="ARBA", uploader_key="arba_minio_file_upload", timeout_key="arba_minio_timeout", button_key="btn_arba_minio_zip")

    # --- AGIP ---
    with subtab_agip:
        st.markdown("#### AGIP - Retenciones y Percepciones IIBB")
        agip_mode = st.radio("Modo", ["Individual", "Masiva"], key="agip_mode", horizontal=True)
        agip_minio = st.checkbox("Carga a MinIO", value=True, key="agip_minio_option")
        agip_proxy = st.checkbox("Usar proxy_request", value=False, key="agip_proxy_option")
        if agip_mode == "Individual":
            agip_usuario = st.text_input("Usuario", value="", key="agip_usuario_ind")
            agip_clave = st.text_input("Clave", value="", type="password", key="agip_clave_ind")
            agip_cuit_repr = st.text_input("CUIT representado", value="", key="agip_cuit_repr_ind")
            agip_denominacion = st.text_input("Denominacion", value="", key="agip_denominacion_ind")
            agip_desde = st.text_input("Desde (ej: 20240101)", value="", key="agip_desde_ind")
            agip_hasta = st.text_input("Hasta (ej: 20241231)", value="", key="agip_hasta_ind")
            if st.button("Consultar AGIP", key="btn_agip_consulta_ind"):
                if not (agip_usuario.strip() and agip_clave.strip() and agip_cuit_repr.strip() and agip_desde.strip() and agip_hasta.strip()):
                    st.warning("Completa todos los campos obligatorios.")
                else:
                    headers_local = build_headers(x_api_key, header_email)
                    payload_agip = {
                        "usuario": agip_usuario.strip(), "clave": agip_clave,
                        "cuit_representado": agip_cuit_repr.strip(),
                        "denominacion": agip_denominacion.strip(),
                        "desde": agip_desde.strip(), "hasta": agip_hasta.strip(),
                        "carga_minio": bool(agip_minio), "proxy_request": bool(agip_proxy)
                    }
                    with st.spinner("Consultando AGIP..."):
                        resp_agip = call_agip_consulta(base_url, headers_local, payload_agip)
                    st.info(f"HTTP status: {resp_agip.get('http_status')}")
                    st.json(resp_agip.get("data"))
                    st.session_state["agip_last_response"] = resp_agip.get("data")
                    st.session_state["agip_last_cuit"] = agip_cuit_repr.strip()
            last_agip_data = st.session_state.get("agip_last_response")
            last_agip_cuit = st.session_state.get("agip_last_cuit", "").strip()
            if last_agip_data is not None and last_agip_cuit:
                if st.button("Generar ZIP con archivos MinIO", key="btn_agip_zip_ind"):
                    with st.spinner("Descargando archivos desde MinIO..."):
                        zip_bytes, log_df = download_minio_to_zip_by_contributor(
                            [{"cuit_representado": last_agip_cuit, "data": json.dumps(last_agip_data, ensure_ascii=False)}],
                            url_field="data", contributor_field="cuit_representado"
                        )
                    if len(log_df) > 0:
                        st.success(f"ZIP generado: {len(log_df)} operaciones")
                        col1, col2 = st.columns(2)
                        with col1:
                            st.download_button(label="Descargar ZIP", data=zip_bytes, file_name=f"agip_{last_agip_cuit}_{date.today().strftime('%Y%m%d')}.zip", mime="application/zip", key="download_agip_zip_ind")
                        with col2:
                            log_xlsx = make_output_excel(log_df, sheet_name="Log")
                            st.download_button(label="Log", data=log_xlsx, file_name=f"log_agip_{last_agip_cuit}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_agip_log_ind")
                    else:
                        st.info("No se encontraron URLs de MinIO en la respuesta.")
        else:
            render_minio_mass_download(section_title="AGIP", uploader_key="agip_minio_file_upload", timeout_key="agip_minio_timeout", button_key="btn_agip_minio_zip")

    # --- Misiones ---
    with subtab_misiones:
        st.markdown("#### Misiones - Retenciones y Percepciones IIBB")
        mis_mode = st.radio("Modo", ["Individual", "Masiva"], key="mis_mode", horizontal=True)
        mis_minio = st.checkbox("Carga a MinIO", value=True, key="mis_minio_option")
        mis_proxy = st.checkbox("Usar proxy_request", value=False, key="mis_proxy_option")
        if mis_mode == "Individual":
            mis_cuit_rep = st.text_input("CUIT representante", value="", key="mis_cuit_rep_ind")
            mis_clave = st.text_input("Clave representante", value="", type="password", key="mis_clave_ind")
            mis_cuit_repr = st.text_input("CUIT representado", value="", key="mis_cuit_repr_ind")
            mis_denominacion = st.text_input("Denominacion", value="", key="mis_denominacion_ind")
            mis_desde = st.text_input("Desde (ej: 20240101)", value="", key="mis_desde_ind")
            mis_hasta = st.text_input("Hasta (ej: 20241231)", value="", key="mis_hasta_ind")
            if st.button("Consultar Misiones", key="btn_mis_consulta_ind"):
                if not (mis_cuit_rep.strip() and mis_clave.strip() and mis_cuit_repr.strip() and mis_desde.strip() and mis_hasta.strip()):
                    st.warning("Completa todos los campos obligatorios.")
                else:
                    headers_local = build_headers(x_api_key, header_email)
                    payload_mis = {
                        "cuit_representante": mis_cuit_rep.strip(),
                        "clave_representante": mis_clave,
                        "cuit_representado": mis_cuit_repr.strip(),
                        "denominacion": mis_denominacion.strip(),
                        "desde": mis_desde.strip(), "hasta": mis_hasta.strip(),
                        "carga_minio": bool(mis_minio), "proxy_request": bool(mis_proxy)
                    }
                    with st.spinner("Consultando Misiones..."):
                        resp_mis = call_misiones_consulta(base_url, headers_local, payload_mis)
                    st.info(f"HTTP status: {resp_mis.get('http_status')}")
                    st.json(resp_mis.get("data"))
                    st.session_state["mis_last_response"] = resp_mis.get("data")
                    st.session_state["mis_last_cuit"] = mis_cuit_repr.strip()
            last_mis_data = st.session_state.get("mis_last_response")
            last_mis_cuit = st.session_state.get("mis_last_cuit", "").strip()
            if last_mis_data is not None and last_mis_cuit:
                if st.button("Generar ZIP con archivos MinIO", key="btn_mis_zip_ind"):
                    with st.spinner("Descargando archivos desde MinIO..."):
                        zip_bytes, log_df = download_minio_to_zip_by_contributor(
                            [{"cuit_representado": last_mis_cuit, "data": json.dumps(last_mis_data, ensure_ascii=False)}],
                            url_field="data", contributor_field="cuit_representado"
                        )
                    if len(log_df) > 0:
                        st.success(f"ZIP generado: {len(log_df)} operaciones")
                        col1, col2 = st.columns(2)
                        with col1:
                            st.download_button(label="Descargar ZIP", data=zip_bytes, file_name=f"misiones_{last_mis_cuit}_{date.today().strftime('%Y%m%d')}.zip", mime="application/zip", key="download_mis_zip_ind")
                        with col2:
                            log_xlsx = make_output_excel(log_df, sheet_name="Log")
                            st.download_button(label="Log", data=log_xlsx, file_name=f"log_misiones_{last_mis_cuit}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_mis_log_ind")
                    else:
                        st.info("No se encontraron URLs de MinIO en la respuesta.")
        else:
            render_minio_mass_download(section_title="Misiones", uploader_key="mis_minio_file_upload", timeout_key="mis_minio_timeout", button_key="btn_mis_minio_zip")

# ====================================================================
# TAB: SRT Alicuotas
# ====================================================================
with tab_srt_alicuotas:
    st.subheader("SRT Alicuotas ART")

    from api.endpoints.srt_alicuotas import normalize_srt_consulta_rows, build_srt_excel

    st.markdown("### SRT Alicuotas ART")
    st.write("Consulta las alicuotas de ART (Aseguradoras de Riesgos del Trabajo) para uno o varios CUITs.")
    srt_mode = st.radio("Modo de consulta", ["Individual", "Masiva"], key="srt_mode", horizontal=True)
    srt_proxy = st.checkbox("Usar proxy_request", value=False, key="srt_proxy_option")
    if srt_mode == "Individual":
        srt_cuit_login = st.text_input("CUIT login", value="", key="srt_cuit_login_ind")
        srt_clave = st.text_input("Clave", value="", type="password", key="srt_clave_ind")
        srt_cuits = st.text_area("CUITs a consultar (separados por comas o saltos de linea)", value="", height=100, key="srt_cuits_ind")
        if st.button("Consultar SRT Alicuotas", key="btn_srt_consulta_ind"):
            if not (srt_cuit_login.strip() and srt_clave.strip() and srt_cuits.strip()):
                st.warning("Completa todos los campos obligatorios.")
            else:
                cuits_consulta = [c.strip() for c in re.split(r"[,;\n\t ]+", srt_cuits) if c.strip()]
                headers_local = build_headers(x_api_key, header_email)
                payload_srt = {
                    "cuit_login": srt_cuit_login.strip(), "clave": srt_clave,
                    "cuits_consulta": cuits_consulta,
                    "proxy_request": bool(srt_proxy)
                }
                with st.spinner("Consultando SRT Alicuotas..."):
                    resp_srt = call_srt_alicuotas_consulta(base_url, headers_local, payload_srt)
                st.info(f"HTTP status: {resp_srt.get('http_status')}")
                data_srt = resp_srt.get("data")
                st.json(data_srt)
                if isinstance(data_srt, dict):
                    consultas = data_srt.get("consultas")
                    if consultas:
                        rows = normalize_srt_consulta_rows(consultas)
                        if rows:
                            st.write("### Resultado formateado (vista previa)")
                            df_srt = pd.DataFrame(rows)
                            st.dataframe(df_srt.head(50), use_container_width=True)
                            xlsx_srt = build_srt_excel(rows)
                            if xlsx_srt:
                                st.download_button(label="Descargar Excel SRT", data=xlsx_srt, file_name=f"srt_alicuotas_{date.today().strftime('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_srt_ind")
    else:
        st.markdown("#### Consulta masiva SRT Alicuotas")
        st.write("Subi un Excel con: **cuit_login**, **clave**, **cuits_consulta** (separados por coma), **proxy_request** (opcional)")
        srt_file = st.file_uploader("Archivo Excel", type=["xlsx"], key="srt_file_upload")
        if srt_file:
            try:
                df_srt_in = pd.read_excel(srt_file, dtype=str).fillna("")
            except Exception as e:
                st.error(f"Error leyendo Excel: {e}")
                df_srt_in = pd.DataFrame()
            df_srt_in.columns = [c.strip().lower() for c in df_srt_in.columns]
            required = ["cuit_login", "clave", "cuits_consulta"]
            missing = [c for c in required if c not in df_srt_in.columns]
            if missing:
                st.error(f"Faltan columnas: {', '.join(missing)}")
            else:
                st.success(f"Filas leidas: {len(df_srt_in)}")
                with st.expander("Vista previa"):
                    st.dataframe(df_srt_in.head(10), use_container_width=True)
                if st.button("Procesar SRT Alicuotas masivo", key="btn_srt_masivo"):
                    headers_local = build_headers(x_api_key, header_email)
                    all_rows = []
                    progress = st.progress(0)
                    status_ph = st.empty()
                    for idx, row in df_srt_in.reset_index(drop=True).iterrows():
                        cuits = [c.strip() for c in re.split(r"[,;|\n\t ]+", str(row["cuits_consulta"])) if c.strip()]
                        proxy = parse_bool_cell(row.get("proxy_request"), False) if "proxy_request" in df_srt_in.columns else srt_proxy
                        status_ph.info(f"Procesando {idx+1}/{len(df_srt_in)} - {row['cuit_login']}")
                        payload = {
                            "cuit_login": row["cuit_login"].strip(), "clave": row["clave"],
                            "cuits_consulta": cuits,
                            "proxy_request": proxy
                        }
                        resp = call_srt_alicuotas_consulta(base_url, headers_local, payload)
                        data = resp.get("data")
                        if isinstance(data, dict):
                            consultas = data.get("consultas")
                            if consultas:
                                rows = normalize_srt_consulta_rows(consultas)
                                for r in rows:
                                    r["cuit_login"] = row["cuit_login"]
                                all_rows.extend(rows)
                        progress.progress(int((idx + 1) / len(df_srt_in) * 100))
                    status_ph.success("Procesamiento finalizado.")
                    if all_rows:
                        df_all = pd.DataFrame(all_rows)
                        st.dataframe(df_all.head(50), use_container_width=True)
                        xlsx_bytes = build_srt_excel(all_rows)
                        st.download_button(label="Descargar Excel consolidado SRT", data=xlsx_bytes, file_name=f"srt_alicuotas_consolidado_{date.today().strftime('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_srt_masivo")
                    else:
                        st.info("No se encontraron resultados para procesar.")

# ====================================================================
# TAB: BCRA
# ====================================================================
with tab_bcra:
    st.subheader("Banco Central (BCRA)")
    st.markdown("### BCRA - Consultas a la API del Banco Central")
    st.write("Selecciona una operacion y completa los parametros requeridos.")

    choices = get_bcra_operation_choices()
    choice_map = {label: op_id for op_id, label in choices}
    selected_label = st.selectbox("Operacion", list(choice_map.keys()), key="bcra_operation")
    selected_op = choice_map[selected_label]

    from api.endpoints.bcra import BCRA_OPERATIONS
    spec = BCRA_OPERATIONS[selected_op]
    show_group = spec.get("group", "")
    st.caption(f"Grupo: {show_group}")

    params: Dict[str, Any] = {}
    st.markdown("#### Parametros requeridos")
    for field in spec.get("required", []):
        params[field] = st.text_input(f"{field} (*)", value="", key=f"bcra_req_{field}")
    if spec.get("optional"):
        st.markdown("#### Parametros opcionales")
        for field in spec["optional"]:
            val = st.text_input(f"{field}", value="", key=f"bcra_opt_{field}")
            if val.strip():
                params[field] = val.strip()

    timeout_bcra = st.number_input("Timeout (segundos)", min_value=10, value=60, step=10, key="bcra_timeout")

    if st.button("Consultar BCRA", key="btn_bcra_consulta"):
        missing = [f for f in spec.get("required", []) if f not in params or not str(params[f]).strip()]
        if missing:
            st.warning(f"Faltan parametros requeridos: {', '.join(missing)}")
        else:
            with st.spinner(f"Consultando {selected_label}..."):
                result = run_bcra_operation(selected_op, params, timeout_sec=int(timeout_bcra))
            http_status = result.get("http_status")
            st.info(f"HTTP status: {http_status}")
            if http_status == 200:
                data = result.get("data")
                st.json(data)
                flat_rows = flatten_bcra_results(selected_op, data)
                if flat_rows:
                    st.write("### Resultado formateado (vista previa)")
                    st.dataframe(pd.DataFrame(flat_rows).head(100), use_container_width=True)
                    xlsx_bcra = make_output_excel(pd.DataFrame(flat_rows), sheet_name="BCRA")
                    st.download_button(label="Descargar Excel BCRA", data=xlsx_bcra, file_name=f"bcra_{selected_op}_{date.today().strftime('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_bcra_ind")
            else:
                st.json(result.get("data", result))

    st.markdown("---")
    st.markdown("#### Consulta masiva BCRA")
    st.write("Subi un Excel con las columnas de parametros requeridos (y opcionales) para la operacion seleccionada.")
    bcra_file = st.file_uploader("Archivo Excel", type=["xlsx"], key="bcra_file_upload")
    if bcra_file:
        try:
            df_bcra = pd.read_excel(bcra_file, dtype=str).fillna("")
        except Exception as e:
            st.error(f"Error leyendo Excel: {e}")
            df_bcra = pd.DataFrame()
        if not df_bcra.empty and st.button("Procesar BCRA masivo", key="btn_bcra_masivo"):
            all_results = []
            progress = st.progress(0)
            status_ph = st.empty()
            for idx, row in df_bcra.reset_index(drop=True).iterrows():
                row_params: Dict[str, Any] = {}
                for field in spec.get("required", []):
                    if field in df_bcra.columns:
                        row_params[field] = str(row[field]).strip()
                for field in spec.get("optional", []):
                    if field in df_bcra.columns and str(row[field]).strip():
                        row_params[field] = str(row[field]).strip()
                status_ph.info(f"Procesando {idx+1}/{len(df_bcra)} - {selected_label}")
                try:
                    result = run_bcra_operation(selected_op, row_params, timeout_sec=int(timeout_bcra))
                    all_results.append({"fila": idx + 1, "http_status": result.get("http_status"), "data": json.dumps(result.get("data"), ensure_ascii=False)})
                except Exception as err:
                    all_results.append({"fila": idx + 1, "http_status": None, "data": json.dumps({"error": str(err)})})
                progress.progress(int((idx + 1) / len(df_bcra) * 100))
            status_ph.success("Procesamiento finalizado.")
            if all_results:
                df_out = pd.DataFrame(all_results)
                st.dataframe(df_out.head(50), use_container_width=True)
                xlsx_bcra_m = make_output_excel(df_out, sheet_name="BCRA_Masivo")
                st.download_button(label="Descargar Excel BCRA masivo", data=xlsx_bcra_m, file_name=f"bcra_masivo_{selected_op}_{date.today().strftime('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_bcra_masivo")

# ====================================================================
# TAB: Procesar PEM
# ====================================================================
with tab_procesar_pem:
    st.subheader("Procesar PEM")
    st.markdown("### Procesar PEM")
    st.write("Convierte archivos PEM (certificados) a Excel/JSON/XML usando la API.")
    pem_mode = st.radio("Modo", ["Individual", "Masivo"], key="pem_mode", horizontal=True)
    pem_timeout = st.number_input("Timeout (segundos)", min_value=10, value=120, step=10, key="pem_timeout")
    if pem_mode == "Individual":
        pem_file = st.file_uploader("Archivo .pem", type=["pem"], key="pem_file_ind")
        if pem_file is not None:
            if st.button("Convertir PEM", key="btn_pem_convertir"):
                headers_local = build_headers(x_api_key, header_email)
                with st.spinner("Procesando PEM..."):
                    resp_pem = call_procesar_pem(pem_file.read(), pem_file.name, base_url, headers_local, timeout_sec=int(pem_timeout))
                st.info(f"HTTP status: {resp_pem.get('http_status')}")
                data_pem = resp_pem.get("data")
                st.json(data_pem)
                if resp_pem.get("http_status") == 200 and isinstance(data_pem, dict):
                    try:
                        xlsx_pem = build_pem_excel(data_pem)
                        st.download_button(label="Descargar Excel", data=xlsx_pem, file_name=f"{os.path.splitext(pem_file.name)[0]}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_pem_xlsx")
                    except Exception as e:
                        st.error(f"Error generando Excel: {e}")
                    json_pem = json.dumps(data_pem, ensure_ascii=False, indent=2)
                    st.download_button(label="Descargar JSON", data=json_pem, file_name=f"{os.path.splitext(pem_file.name)[0]}.json", mime="application/json", key="download_pem_json")
    else:
        pem_files = st.file_uploader("Archivos .pem", type=["pem"], accept_multiple_files=True, key="pem_files_mas")
        if pem_files:
            st.success(f"Archivos cargados: {len(pem_files)}")
            if st.button("Procesar PEMs masivo", key="btn_pem_masivo"):
                headers_local = build_headers(x_api_key, header_email)
                out_rows = []
                progress = st.progress(0)
                status_ph = st.empty()
                for idx, pf in enumerate(pem_files):
                    status_ph.info(f"Procesando {idx+1}/{len(pem_files)} - {pf.name}")
                    try:
                        resp = call_procesar_pem(pf.read(), pf.name, base_url, headers_local, timeout_sec=int(pem_timeout))
                        out_rows.append({"archivo": pf.name, "http_status": resp.get("http_status"), "data": json.dumps(resp.get("data"), ensure_ascii=False)})
                    except Exception as e:
                        out_rows.append({"archivo": pf.name, "http_status": None, "data": json.dumps({"error": str(e)})})
                    progress.progress(int((idx + 1) / len(pem_files) * 100))
                status_ph.success("Procesamiento finalizado.")
                if out_rows:
                    df_pem = pd.DataFrame(out_rows)
                    st.dataframe(df_pem.head(50), use_container_width=True)
                    xlsx_pem_m = make_output_excel(df_pem, sheet_name="PEM_Masivo")
                    st.download_button(label="Descargar Excel consolidado PEM", data=xlsx_pem_m, file_name=f"pem_consolidado_{date.today().strftime('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_pem_masivo")

