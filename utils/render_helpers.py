from datetime import date

import streamlit as st

from utils.download import download_minio_to_zip_by_contributor
from utils.excel_utils import make_output_excel, extract_minio_urls_from_excel


def as_ddmmyyyy(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def render_minio_mass_download(section_title: str, uploader_key: str, timeout_key: str, button_key: str) -> None:
    st.markdown(f"#### Descarga masiva {section_title} (solo links MinIO)")
    st.write(
        "Subi un Excel con links MinIO. Si hay columna de contribuyente (ej: `cuit_representado`), "
        "se crearan subcarpetas por contribuyente."
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
