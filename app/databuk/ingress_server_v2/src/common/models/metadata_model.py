from datetime import datetime

from pathlib import Path
from pydantic import BaseModel, Field, field_validator
from common import validation

BASE_DIR = Path(__file__).resolve().parents[2]

class MetadataModel(BaseModel):
    content_type: str = Field(..., description="MIME type: application/json or text/csv")
    node_path: str = Field("", description="Relative path under endpoint (no leading slash)")
    endpoint_name: str = Field(..., description="Logical endpoint name (no slashes)")
    username: str = Field(..., description="Uploader or 'scrapper'")
    received_at: datetime = Field(default_factory=datetime.now(datetime.timezone.utc), description="UTC timestamp (ISO 8601)")
    schema_path: str = Field(..., description="Path to schema file (absolute after validation)")

    @field_validator("content_type")
    @classmethod
    def _validate_content_type(cls, v: str) -> str:
        ok, err = validation.validate_content_type(v)
        if not ok:
            raise ValueError(f"Invalid content type: {err}")
        return v.lower()

    @field_validator("node_path")
    @classmethod
    def _validate_node_path(cls, v: str) -> str:
        safe, err = validation.sanitize_node_path(v)
        if err:
            raise ValueError(f"Invalid node_path: {err}")
        return str(safe)

    @field_validator("endpoint_name")
    @classmethod
    def _validate_endpoint_name(cls, v: str) -> str:
        if not v or "/" in v or "\\" in v:
            raise ValueError("Invalid endpoint name (must not contain '/' or '\\').")
        return v

    @field_validator("schema_path")
    @classmethod
    def _validate_schema_path(cls, v: str) -> str:
        p = (BASE_DIR / v).resolve() if not Path(v).is_absolute() else Path(v).resolve()
        if not p.exists():
            raise ValueError(f"Schema file does not exist: {p}")
        return str(p)
