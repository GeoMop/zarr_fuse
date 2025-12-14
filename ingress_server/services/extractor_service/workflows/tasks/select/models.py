from pydantic import BaseModel, Field

from packages.common.models import MetadataModel

class SelectTaskResult(BaseModel):
    local_path: str = Field(..., description="Path to the selected data file")
    meta: MetadataModel = Field(..., description="Metadata associated with the selected data")
    source_name: str = Field(..., description="Name of the data source")
