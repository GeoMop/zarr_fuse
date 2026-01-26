
def validate_content_type(content_type: str | None) -> tuple[bool, str | None]:
    if not content_type:
        return False, "No Content-Type provided"

    ct = content_type.lower()
    if ("json" not in ct) and ("csv" not in ct) and ("x-hdf" not in ct):
        return False, f"Unsupported Content-Type: {content_type}"
    return True, None

def validate_data(data: bytes) -> tuple[bool, str | None]:
    if not data:
        return False, "No data provided"

    return True, None
