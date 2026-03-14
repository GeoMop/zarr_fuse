import io
import json
import csv
import logging

from pathlib import Path
from .content_type import classify_content_type, SupportedContentType

LOG = logging.getLogger(__name__)


def sanitize_node_path(p: str | None) -> Path | None:
    p = (p or "").strip().lstrip("/")

    if not p:
        return None

    candidate = Path(p)

    if candidate.is_absolute() or any(part == ".." for part in candidate.parts):
        raise ValueError(f"Invalid node_path: {p}")

    return candidate


def validate_payload(
    payload: bytes,
    kind: SupportedContentType,
    content_type: str
) -> None:
    if not payload:
        raise ValueError(f"Empty payload with Content-Type: {content_type}")

    match kind:
        case SupportedContentType.JSON:
            try:
                json.loads(payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError(f"Invalid JSON payload: {exc}") from exc

        case SupportedContentType.CSV:
            try:
                reader = csv.reader(io.StringIO(payload.decode("utf-8")))
                next(reader)
            except (UnicodeDecodeError, csv.Error, StopIteration) as exc:
                raise ValueError(f"Invalid CSV payload: {exc}") from exc

        case SupportedContentType.GRIB | SupportedContentType.GRIB_BZ2 | SupportedContentType.OCTET_STREAM:
            LOG.debug("Validation for binary content type %s is not needed", content_type)

        case _:
            raise ValueError(f"Unsupported content type: {content_type}")


def validate_response(payload: bytes, content_type: str) -> None:
    ct = classify_content_type(content_type)
    if ct is None:
        raise ValueError(f"Unsupported Content-Type: {content_type}")

    validate_payload(payload, ct, content_type)
