from typing import Any, Optional


def parse_bool_cell(value: Any, default: bool = False) -> bool:
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


def normalize_contributor_id(value: Any) -> str:
    text = str(value).strip() if value is not None else ""
    return text if text else "sin_identificar"
