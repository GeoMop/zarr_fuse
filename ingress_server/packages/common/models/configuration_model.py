from typing import Literal, Union

from pydantic import BaseModel, Field, field_validator

class S3Config(BaseModel):
    access_key: str = Field(..., description="S3 access key ID")
    secret_key: str = Field(..., description="S3 secret access key")
    endpoint_url: str = Field(..., description="Custom S3 endpoint URL (if any)")
    region: str | None = Field(None, description="S3 region (if any)")
    store_url: str | None = Field(None, description="S3 store URL (if any)")

class BaseDataSourceConfig(BaseModel):
    name: str = Field(..., description="Unique name of the data source")
    schema_name: str = Field(..., description="Name of the schema to use for this data source")

class EndpointConfig(BaseDataSourceConfig):
    kind: Literal["endpoint"] = "endpoint"
    endpoint: str = Field(..., description="API endpoint path for this data source")

    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, v: str):
        if not v.startswith("/"):
            raise ValueError("Endpoint must start with a '/'")
        return v

class ScrapperConfig(BaseDataSourceConfig):
    kind: Literal["scrapper"] = "scrapper"
    url: str = Field(..., description="URL to scrape data from")
    cron: str= Field(..., description="Cron expression for scheduling the scrapper")
    method: str = Field("GET", description="HTTP method to use for the scrapper")

    @field_validator("cron")
    @classmethod
    def validate_cron(cls, v):
        parts = v.split()
        if len(parts) != 5:
            raise ValueError("Cron expression must have 5 parts (min hour day month dow)")
        return v

UnitedDataSourceConfig = Union[EndpointConfig, ScrapperConfig]
