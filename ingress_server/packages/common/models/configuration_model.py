from pydantic import BaseModel, Field, field_validator

class S3Config(BaseModel):
    access_key: str = Field(..., description="S3 access key ID")
    secret_key: str = Field(..., description="S3 secret access key")
    endpoint_url: str = Field(..., description="Custom S3 endpoint URL (if any)")
    region: str | None = Field(None, description="S3 region (if any)")
    store_url: str | None = Field(None, description="S3 store URL (if any)")


class EndpointConfig(BaseModel):
    name: str = Field(..., description="Logical name of the endpoint (no slashes)")
    endpoint: str = Field(..., description="API endpoint path (must start with '/')")
    schema_name: str = Field(..., description="Name of the schema file (without path)")
    extract_fn: str | None = Field(
        None,
        description="Optional extraction function name to apply after loading"
    )
    fn_module: str | None = Field(
        None,
        description="Optional module name where the extraction function is located"
    )

    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, v: str):
        if not v.startswith("/"):
            raise ValueError("Endpoint must start with a '/'")
        return v

class ActiveScrapperURLParams(BaseModel):
    param_name: str = Field(..., description="Name of the URL parameter")
    column_name: str = Field(..., description="Name of the column to map the parameter to")

class ActiveScrapperHeaders(BaseModel):
    header_name: str = Field(..., description="Name of the HTTP header")
    header_value: str = Field(..., description="Value or placeholder for the header")

class ActiveScrapperConfig(BaseModel):
    name: str = Field(..., description="Name of the scrapper")
    url: str = Field(..., description="URL to scrape data from")
    crons: list[str] = Field(..., description="List of cron expressions for scheduling")
    schema_name: str = Field(..., description="Name of the schema to use for the scrapper")
    extract_fn: str | None = Field(None,description="Name of the extraction function to use (if any)")
    fn_module: str | None = Field(None, description="Module where the extraction function is located (if any)")
    headers: list[ActiveScrapperHeaders] = Field(default_factory=list, description="List of HTTP headers to include in requests")
    url_params: list[ActiveScrapperURLParams] = Field(default_factory=list, description="List of URL parameters to include in requests")
    dataframe_path: str | None = Field(None, description="Path to save the scraped data as a dataframe (if any)")
    dataframe_has_header: bool = Field(True, description="Indicates if the dataframe has a header row")

    @field_validator("crons")
    @classmethod
    def validate_cron(cls, v):
        for cron in v:
            parts = cron.split()
            if len(parts) != 5:
                raise ValueError("Each cron expression must have 5 parts (min hour day month dow)")
        return v
