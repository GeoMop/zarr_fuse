from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

class WeatherVariable(BaseModel):
    """Represents a weather variable/array."""
    name: str = Field(..., description="Name of the weather variable")
    type: str = Field(..., description="Type of the variable (array)")
    shape: List[int] = Field(..., description="Shape/dimensions of the array")
    unit: Optional[str] = Field(None, description="Unit of measurement")
    description: Optional[str] = Field(None, description="Description of the variable")
    coordinates: Optional[List[str]] = Field(None, description="Coordinate names")
    data_type: Optional[str] = Field(None, description="Data type of the array")
    chunk_shape: Optional[List[int]] = Field(None, description="Chunk shape for storage")

class WeatherStructureResponse(BaseModel):
    """Response model for weather structure endpoint."""
    variables: List[WeatherVariable] = Field(..., description="Weather variables")
    store_name: str = Field(..., description="Name of the Zarr store")
    total_variables: int = Field(..., description="Total number of variables")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Store metadata")

class WeatherDataResponse(BaseModel):
    """Response model for individual weather variable data."""
    name: str = Field(..., description="Variable name")
    data: Optional[List[Any]] = Field(None, description="Actual data values")
    shape: List[int] = Field(..., description="Data shape")
    unit: Optional[str] = Field(None, description="Unit of measurement")
    coordinates: Optional[Dict[str, List[Any]]] = Field(None, description="Coordinate values")
