import json
import logging
import zarr_fuse as zf

from pathlib import Path
from urllib.parse import urlparse

from ..models import MetadataModel, DataSourceConfig
from .validate import validate_response
from .files import save_data

LOG = logging.getLogger("io.process")


def process_payload(
    data_source: DataSourceConfig,
    payload: bytes,
    content_type: str,
    username: str,
    node_path: str | None = None,
    dataframe_row: dict | None = None,
) -> tuple[bool, str | None]:
    err = validate_response(payload, content_type)
    if err:
        return False, err

    metadata = MetadataModel.from_data_source(
        data_source,
        content_type=content_type,
        username=username,
        node_path=node_path,
        dataframe_row=dataframe_row,
    )

    err = save_data(
        metadata=metadata,
        payload=payload,
    )
    if err:
        return False, err

    return True, None


def open_root(schema_path: Path) -> tuple[zf.Node | None, str | None]:
    opts = {
        "S3_OPTIONS": json.dumps({
            "config_kwargs": {"s3": {"addressing_style": "path"}}
        }),
    }

    return zf.open_store(schema_path, **opts), None
