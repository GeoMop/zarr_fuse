from pathlib import Path
from pydantic import BaseModel, Field, field_validator

BASE_DIR = Path(__file__).resolve().parents[2]

class S3Config(BaseModel):
    access_key: str = Field(..., description="S3 access key ID")
    secret_key: str = Field(..., description="S3 secret access key")
    endpoint_url: str = Field(..., description="Custom S3 endpoint URL (if any)")
    region: str | None = Field(None, description="S3 region (if any)")
    store_url: str | None = Field(None, description="S3 store URL (if any)")

class AppConfig(BaseModel):
    log_level: str = "INFO"
    queue_mode: str = "s3"
    port: int = 8000
    s3: S3Config

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
        p = (BASE_DIR / v).resolve() if not Path(v).is_absolute() else Path(v)
        if not p.exists():
            raise ValueError(f"Schema file does not exist: {v}")
        return v

class ScrapperConfig(BaseModel):
    name: str
    url: str
    cron: str
    schema_path: str

    @field_validator("schema_path")
    @classmethod
    def must_exist(cls, v):
        p = (BASE_DIR / v).resolve() if not Path(v).is_absolute() else Path(v)
        if not p.exists():
            raise ValueError(f"Schema file not found: {p}")
        return str(p)

    @field_validator("cron")
    @classmethod
    def validate_cron(cls, v):
        parts = v.split()
        if len(parts) != 5:
            raise ValueError("Cron expression must have 5 parts (min hour day month dow)")
        return v
