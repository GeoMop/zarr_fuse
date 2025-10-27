from datetime import datetime

from pydantic import BaseModel, Field, field_validator
from common import validation


class MetadataModel(BaseModel):
    content_type: str = Field(..., description="MIME type: application/json or text/csv")
    node_path: str = Field("", description="Relative path under endpoint (no leading slash)")
    endpoint_name: str = Field(..., description="Logical endpoint name (no slashes)")
    username: str = Field(..., description="Uploader or 'scrapper'")
    received_at: datetime = Field(
        default_factory=lambda: datetime.utcnow(),
        description="UTC timestamp (ISO 8601)"
    )
    schema_name: str = Field(..., description="Name of the schema file (without path)")

    @field_validator("content_type")
    @classmethod
    def _validate_content_type(cls, v: str) -> str:
        ok, err = validation.validate_content_type(v)
        if not ok:
            raise ValueError(f"Invalid content type: {err}")
        return v.lower()

    @field_validator("endpoint_name")
    @classmethod
    def _validate_endpoint_name(cls, v: str) -> str:
        if not v or "/" in v or "\\" in v:
            raise ValueError("Invalid endpoint name (must not contain '/' or '\\').")
        return v
