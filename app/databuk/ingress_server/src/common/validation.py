import io
import json
import csv
import polars as pl

def read_df_from_bytes(data: bytes, content_type: str) -> tuple[pl.DataFrame | None, str | None]:
    ct = content_type.lower()
    if "csv" in ct:
        return pl.read_csv(io.BytesIO(data)), None
    elif "json" in ct:
        return pl.read_json(io.BytesIO(data)), None
    else:
        return None, f"Unsupported content type: {content_type}. Use application/json or text/csv."

def validate_content_type(content_type: str | None) -> tuple[bool, str | None]:
    if not content_type:
        return False, "No Content-Type provided"

    ct = content_type.lower()
    if ("application/json" not in ct) and ("text/csv" not in ct):
        return False, f"Unsupported Content-Type: {content_type}"
    return True, None

def validate_data(data: bytes, content_type: str) -> tuple[bool, str | None]:
    if not data:
        return False, "No data provided"

    if "json" in content_type:
        try:
            json.loads(data.decode("utf-8"))
        except Exception as e:
            return False, f"Invalid JSON payload: {e}"

    elif "csv" in content_type:
        try:
            reader = csv.reader(io.StringIO(data.decode("utf-8")))
            next(reader)
        except Exception as e:
            return False, f"Invalid CSV payload: {e}"

    return True, None
