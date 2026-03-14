from enum import Enum
from pathlib import Path
from typing import Literal, Any

from pydantic import BaseModel, Field, model_validator

from ..models import DataSourceConfig, ConfigurationError, resolve_path


class RenderSource(str, Enum):
    DATETIME_UTC = "datetime_utc"
    DATETIME_LOCAL = "datetime_local"
    CONST = "const"


class IterateSource(str, Enum):
    SCHEMA = "schema"
    DATAFRAME = "dataframe"


class RunConfig(BaseModel):
    cron: str
    set: dict[str, str] = Field(default_factory=dict)


class ActiveScrapperHeader(BaseModel):
    header_name: str
    header_value: str


class QueryParamConfig(BaseModel):
    name: str
    value: str


class RequestConfig(BaseModel):
    method: Literal["GET"] = "GET"
    url: str
    headers: list[ActiveScrapperHeader] = Field(default_factory=list)
    query_params: list[QueryParamConfig] = Field(default_factory=list)


class RenderValue(BaseModel):
    name: str
    source: RenderSource
    format: str | None = None
    value: str | None = None

    @model_validator(mode="after")
    def _validate_variables(self):
        if self.source == RenderSource.CONST:
            if self.value is None:
                raise ConfigurationError("render source=const requires 'value'")
            if self.format is not None:
                raise ConfigurationError("render source=const does not support 'format'")

        elif self.source in {RenderSource.DATETIME_UTC, RenderSource.DATETIME_LOCAL}:
            if self.value is not None:
                raise ConfigurationError(f"render source={self.source} does not support 'value'")
            if self.format is None:
                raise ConfigurationError(f"render source={self.source} requires 'format' (strftime)")

        return self


class IterateSchemaConfig(BaseModel):
    name: str
    source: IterateSource = IterateSource.SCHEMA
    target_node: str | None = None
    schema_regex: str
    unique: bool = True


class IterateDataframeConfig(BaseModel):
    name: str
    source: IterateSource = IterateSource.DATAFRAME
    dataframe_path: str
    dataframe_has_header: bool = True
    outputs: dict[str, str] = Field(default_factory=dict)

    def resolve_dataframe_path(self, config_dir: Path) -> Path:
        return resolve_path(self.dataframe_path, config_dir)


IterateConfig = IterateSchemaConfig | IterateDataframeConfig


class ActiveScrapperConfig(BaseModel):
    data_source: DataSourceConfig
    runs: list[RunConfig]
    request: RequestConfig
    render: list[RenderValue] = Field(default_factory=list)
    iterate: list[IterateConfig] = Field(default_factory=list)

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

    @model_validator(mode="after")
    def _validate_variables(self) -> "ActiveScrapperConfig":
        render_names = {r.name for r in self.render}
        iterate_names = {i.name for i in self.iterate}

        run_keys: set[str] = set()
        for r in self.runs:
            run_keys.update(r.set.keys())

        overlaps = (
            (render_names & iterate_names)
            | (render_names & run_keys)
            | (iterate_names & run_keys)
        )
        if overlaps:
            raise ConfigurationError(f"Variables defined in multiple places: {sorted(overlaps)}")

        return self
