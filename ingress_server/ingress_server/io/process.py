import logging

from ..models import MetadataModel, DataSourceConfig
from ..app_config import AppConfig
from .validate import validate_response
from .files import save_data

LOG = logging.getLogger(__name__)


def process_payload(
    app_config: AppConfig,
    data_source: DataSourceConfig,
    payload: bytes,
    content_type: str,
    username: str,
    node_path: str | None = None,
    dataframe_row: dict | None = None,
) -> None:
    validate_response(payload, content_type)

    metadata = MetadataModel.from_data_source(
        data_source,
        content_type=content_type,
        username=username,
        node_path=node_path,
        dataframe_row=dataframe_row
    )

    save_data(
        app_config=app_config,
        metadata=metadata,
        payload=payload,
    )
