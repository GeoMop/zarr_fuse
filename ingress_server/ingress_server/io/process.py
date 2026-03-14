import logging

from ..models import MetadataModel, DataSourceConfig
from ..app_config import AppConfig
from .validate import validate_response
from .files import save_data

LOG = logging.getLogger("io.process")


def process_payload(
    app_config: AppConfig,
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
        app_config=app_config,
        metadata=metadata,
        payload=payload,
    )
    if err:
        return False, err

    return True, None
