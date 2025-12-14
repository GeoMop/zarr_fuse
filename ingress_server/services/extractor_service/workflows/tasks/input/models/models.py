from pydantic import BaseModel, Field

from packages.common.models import MetadataModel

class InputTaskResult(BaseModel):
    bucket: str = Field(description="S3 bucket name")
    data_key: str = Field(description="S3 key of the data object")
    meta_key: str = Field(description="S3 key of the metadata object")
    data_path: str = Field(description="Path to processed data artifact")
    metadata: MetadataModel = Field(description="Validated metadata dict")
