from pathlib import Path
from pydantic import BaseModel, Field, field_validator

BASE_DIR = Path(__file__).resolve().parents[3]

class S3Config(BaseModel):
    access_key: str = Field(..., description="S3 access key ID")
    secret_key: str = Field(..., description="S3 secret access key")
    endpoint_url: str = Field(..., description="Custom S3 endpoint URL (if any)")
    region: str | None = Field(None, description="S3 region (if any)")
    store_url: str | None = Field(None, description="S3 store URL (if any)")

class EndpointConfig(BaseModel):
    name: str
    endpoint: str
    schema_path: str

    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, v: str):
        if not v.startswith("/"):
            raise ValueError("Endpoint must start with a '/'")
        return v

    @field_validator("schema_path")
    @classmethod
    def validate_schema_path(cls, v: str):
        path = Path(BASE_DIR / "inputs" / v)
        if not path.exists():
            raise ValueError(f"Schema file does not exist: {path}")
        return str(path)

class ScrapperConfig(BaseModel):
    name: str
    url: str
    cron: str
    schema_path: str
    method: str = Field("GET", description="HTTP method to use for the scrapper")

    @field_validator("schema_path")
    @classmethod
    def must_exist(cls, v):
        path = Path(BASE_DIR / "inputs" / v)
        if not path.exists():
            raise ValueError(f"Schema file not found: {path}")
        return str(path)

    @field_validator("cron")
    @classmethod
    def validate_cron(cls, v):
        parts = v.split()
        if len(parts) != 5:
            raise ValueError("Cron expression must have 5 parts (min hour day month dow)")
        return v
