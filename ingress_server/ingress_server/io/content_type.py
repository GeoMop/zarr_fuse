from enum import Enum


class SupportedContentType(str, Enum):
    JSON = "json"
    CSV = "csv"
    GRIB = "grib"
    GRIB_BZ2 = "grib_bz2"
    OCTET_STREAM = "octet-stream"

def classify_content_type(content_type: str | None) -> SupportedContentType | None:
    if content_type is None:
        return None
    ct = content_type.lower()

    if "json" in ct:
        return SupportedContentType.JSON
    if "csv" in ct:
        return SupportedContentType.CSV

    is_grib = ("grib" in ct) or ("grb" in ct)
    is_bz2 = ("bz2" in ct) or ("bzip2" in ct)

    if is_grib and is_bz2:
        return SupportedContentType.GRIB_BZ2
    if is_grib:
        return SupportedContentType.GRIB
    if "octet-stream" in ct:
        return SupportedContentType.OCTET_STREAM

    return None

def get_content_type_suffix(kind: SupportedContentType) -> str:
    match kind:
        case SupportedContentType.JSON:
            return ".json"
        case SupportedContentType.CSV:
            return ".csv"
        case SupportedContentType.GRIB:
            return ".grib"
        case SupportedContentType.GRIB_BZ2:
            return ".grib.bz2"
        case SupportedContentType.OCTET_STREAM:
            return ".bin"
