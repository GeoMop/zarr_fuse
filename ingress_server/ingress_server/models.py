import time

from typing import Any
from pathlib import Path
from pydantic import BaseModel, Field, model_validator


class ConfigurationError(ValueError):
    """Raised when active scrapper configuration is invalid."""


def resolve_path(path_str: str, config_dir: Path) -> Path:
    if not path_str or not path_str.strip():
        raise ConfigurationError("Path must not be empty")

    path = Path(path_str)
    if path.is_absolute():
        return path
    return config_dir / path


class DataSourceConfig(BaseModel):
    name: str
    schema_path: str
    target_node: str | None = None
    extract_fn: str | None = None
    fn_module: str | None = None

    def resolve_schema_path(self, config_dir: Path) -> Path:
        return resolve_path(self.schema_path, config_dir)

class EndpointConfig(BaseModel):
    data_source: DataSourceConfig
    endpoint: str

    @model_validator(mode="before")
    @classmethod
    def _inflate_data_source(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        if "data_source" in data:
            return data

        return {
            **data,
            "data_source": {
                "name": data.get("name"),
                "target_node": data.get("target_node"),
                "schema_path": data.get("schema_path"),
                "extract_fn": data.get("extract_fn"),
                "fn_module": data.get("fn_module"),
            },
        }

    @property
    def name(self) -> str:
        return self.data_source.name

    @property
    def target_node(self) -> str | None:
        return self.data_source.target_node

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
    target_node: str | None = None
    config_dir: Path | None = None

    def resolve_schema_path(self, config_dir: Path) -> Path:
        return resolve_path(self.schema_path, config_dir)

    @classmethod
    def from_data_source(
        cls,
        data_source: DataSourceConfig,
        *,
        content_type: str,
        username: str,
        node_path: str | None = None,
        dataframe_row: dict | None = None,
        config_dir: Path | None = None,
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
            target_node=data_source.target_node,
            config_dir=config_dir,
        )
