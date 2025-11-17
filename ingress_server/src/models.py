from pydantic import BaseModel, Field

class ActiveScrapperURLParams(BaseModel):
    param_name: str
    column_name: str

class ActiveScrapperHeaders(BaseModel):
    header_name: str
    header_value: str

class ActiveScrapperConfig(BaseModel):
    name: str
    crons: list[str]
    url: str
    schema_path: str
    extract_fn: str | None = None
    fn_module: str | None = None
    headers: list[ActiveScrapperHeaders] = Field(default_factory=list)
    url_params: list[ActiveScrapperURLParams] = Field(default_factory=list)
    dataframe_path: str | None = None
    dataframe_has_header: bool = True

class EndpointConfig(BaseModel):
    name: str
    endpoint: str
    schema_path: str
    extract_fn: str | None = None
    fn_module: str | None = None

class MetadataModel(BaseModel):
    content_type: str
    endpoint_name: str
    node_path: str | None
    username: str
    received_at: str
    schema_path: str
    extract_fn: str | None
    fn_module: str | None
    dataframe_row: dict | None
