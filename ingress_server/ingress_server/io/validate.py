import io
import json
import csv
import logging

from pathlib import Path
from .content_type import classify_content_type, SupportedContentType

LOG = logging.getLogger("io.validate")


def sanitize_node_path(p: str | None) -> Path | None:
    p = (p or "").strip().lstrip("/")

    if not p:
        return None

    candidate = Path(p)

    if candidate.is_absolute() or any(part == ".." for part in candidate.parts):
        raise ValueError(f"Invalid node_path: {p}")

    return candidate


def validate_payload(payload: bytes, kind: SupportedContentType, content_type: str) -> str | None:
    if not payload:
        return f"Empty payload with Content-Type: {content_type}"

    match kind:
        case SupportedContentType.JSON:
            try:
                json.loads(payload.decode("utf-8"))
            except Exception as e:
                return f"Invalid JSON payload: {e}"

        case SupportedContentType.CSV:
            try:
                reader = csv.reader(io.StringIO(payload.decode("utf-8")))
                next(reader)
            except Exception as e:
                return f"Invalid CSV payload: {e}"

        case SupportedContentType.GRIB | SupportedContentType.GRIB_BZ2 | SupportedContentType.OCTET_STREAM:
            LOG.debug("Validation for binary content type %s is not needed", content_type)
            return None

        case _:
            return f"Unsupported content type: {content_type}"

    return None


def validate_response(payload: bytes, content_type: str) -> str | None:
    ct = classify_content_type(content_type)
    if ct is None:
        return f"Unsupported Content-Type: {content_type}"

    err = validate_payload(payload, ct, content_type)
    if err:
        LOG.error("Payload validation failed for Content-Type: %s, error: %s", content_type, err)
        return f"Payload validation failed for Content-Type: {content_type}, error: {err}"
    return None
