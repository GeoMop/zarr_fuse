from pydantic import BaseModel, Field, field_validator

class S3Config(BaseModel):
    access_key: str = Field(..., description="S3 access key ID")
    secret_key: str = Field(..., description="S3 secret access key")
    endpoint_url: str = Field(..., description="Custom S3 endpoint URL (if any)")
    region: str | None = Field(None, description="S3 region (if any)")
    store_url: str | None = Field(None, description="S3 store URL (if any)")


class EndpointConfig(BaseModel):
    name: str
    endpoint: str
    schema_name: str

    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, v: str):
        if not v.startswith("/"):
            raise ValueError("Endpoint must start with a '/'")
        return v


class ScrapperConfig(BaseModel):
    name: str
    url: str
    cron: str
    schema_name: str
    method: str = Field("GET", description="HTTP method to use for the scrapper")

    @field_validator("cron")
    @classmethod
    def validate_cron(cls, v):
        parts = v.split()
        if len(parts) != 5:
            raise ValueError("Cron expression must have 5 parts (min hour day month dow)")
        return v
