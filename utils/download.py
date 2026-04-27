import os
import zipfile
from io import BytesIO
from typing import Any, Dict, List, Tuple

import pandas as pd
import requests

from utils.file_utils import (
    sanitize_filename,
    infer_filename_from_url,
    get_filename_from_headers,
    is_zip_bytes,
    write_unique,
)


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
            from utils.parse_utils import normalize_contributor_id
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


def download_minio_to_zip_by_contributor(
    data_rows: List[Dict[str, Any]],
    url_field: str,
    contributor_field: str,
    timeout_sec: int = 120
) -> Tuple[bytes, pd.DataFrame]:
    import json
    import re

    from utils.parse_utils import normalize_contributor_id

    log_rows = []
    zip_buffer = BytesIO()

    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for row in data_rows:
            contributor_id = normalize_contributor_id(row.get(contributor_field))

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
                        if isinstance(v, str):
                            collect_urls(v)
                        else:
                            collect_urls(v)
                    return
                if isinstance(obj, list):
                    for item in obj:
                        collect_urls(item)
                    return

            if isinstance(url_value, str):
                try:
                    parsed = json.loads(url_value)
                    collect_urls(parsed)
                except Exception:
                    collect_urls(url_value)
            else:
                collect_urls(url_value)

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


def download_minio_links_to_zip(
    urls: List[str],
    folder: str = "MinIO",
    timeout_sec: int = 120
) -> Tuple[bytes, pd.DataFrame]:
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
