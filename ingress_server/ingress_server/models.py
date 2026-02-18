import time
from pathlib import Path
from pydantic import BaseModel, Field, model_validator

from .configs import get_settings


class DataSourceConfig(BaseModel):
    name: str
    schema_path: str
    schema_node: str | None = None
    extract_fn: str | None = None
    fn_module: str | None = None

    def get_schema_path(self) -> Path:
        path = Path(self.schema_path)
        if path.is_absolute():
            return path
        return get_settings().config_dir / path

class EndpointConfig(BaseModel):
    data_source: DataSourceConfig
    endpoint: str

    @model_validator(mode="before")
    @classmethod
    def _inflate_data_source(cls, data):
        if isinstance(data, dict) and "data_source" not in data:
            data = {**data, "data_source": {
                "name": data.get("name"),
                "schema_path": data.get("schema_path"),
                "extract_fn": data.get("extract_fn"),
                "fn_module": data.get("fn_module"),
                "schema_node": data.get("schema_node"),
            }}
        return data

    @property
    def name(self) -> str:
        return self.data_source.name

    @property
    def schema_path(self) -> str:
        return self.data_source.schema_path

    @property
    def extract_fn(self) -> str | None:
        return self.data_source.extract_fn

    @property
    def fn_module(self) -> str | None:
        return self.data_source.fn_module

class MetadataModel(BaseModel):
    content_type: str
    endpoint_name: str
    node_path: str | None
    username: str
    schema_path: str
    extract_fn: str | None
    fn_module: str | None
    received_at: str = Field(
        default_factory=lambda: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        frozen=True,
        description="Timestamp when the data was received",
    )
    dataframe_row: dict | None
    schema_node: str | None = None

    def get_schema_path(self) -> Path:
        path = Path(self.schema_path)
        if path.is_absolute():
            return path
        return get_settings().config_dir / path

    @classmethod
    def from_data_source(
        cls,
        data_source: DataSourceConfig,
        *,
        content_type: str,
        username: str,
        node_path: str | None = None,
        dataframe_row: dict | None = None,
    ) -> "MetadataModel":
        return cls(
            content_type=content_type,
            endpoint_name=data_source.name,
            node_path=node_path,
            username=username,
            schema_path=data_source.schema_path,
            extract_fn=data_source.extract_fn,
            fn_module=data_source.fn_module,
            dataframe_row=dataframe_row,
            schema_node=data_source.schema_node,
        )
