from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from pathlib import Path

from models.weather import WeatherStructureResponse, WeatherDataResponse
from services.weather_service import WeatherService
from core.config import settings

router = APIRouter(prefix="/weather", tags=["weather"])

@router.get("/structure", response_model=WeatherStructureResponse)
async def get_weather_structure(store_name: str = Query("structure_weather", description="Name of the Zarr store")):
    """
    Get the complete weather structure for a Zarr store.
    
    Args:
        store_name: Name of the store (default: structure_weather)
        
    Returns:
        WeatherStructureResponse with the complete structure
    """
    try:
        # Get the store path from configuration
        store_path = settings.get_store_path(store_name)
        if not store_path:
            raise HTTPException(
                status_code=404, 
                detail=f"Store '{store_name}' not found in configuration"
            )
        
        if not store_path.exists():
            raise HTTPException(
                status_code=404,
                detail=f"Store path does not exist: {store_path}"
            )
        
        # Create weather service and get structure
        weather_service = WeatherService(store_path)
        variables = weather_service.get_weather_structure()
        
        if variables is None:
            raise HTTPException(
                status_code=500,
                detail="Failed to get weather structure"
            )
        
        # Get store metadata
        metadata = None
        try:
            import zarr
            store = zarr.open_group(str(store_path))
            metadata = store.attrs.asdict()
        except:
            pass
        
        return WeatherStructureResponse(
            variables=variables,
            store_name=store_name,
            total_variables=len(variables),
            metadata=metadata
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

@router.get("/variable", response_model=WeatherDataResponse)
async def get_variable_data(
    variable_name: str = Query(..., description="Name of the weather variable"),
    store_name: str = Query("structure_weather", description="Name of the Zarr store"),
    max_elements: int = Query(100, description="Maximum number of elements to return")
):
    """
    Get detailed data for a specific weather variable.
    
    Args:
        variable_name: Name of the variable
        store_name: Name of the store
        max_elements: Maximum number of elements to return
        
    Returns:
        WeatherDataResponse with variable data
    """
    try:
        # Get the store path from configuration
        store_path = settings.get_store_path(store_name)
        if not store_path:
            raise HTTPException(
                status_code=404,
                detail=f"Store '{store_name}' not found in configuration"
            )
        
        if not store_path.exists():
            raise HTTPException(
                status_code=404,
                detail=f"Store path does not exist: {store_path}"
            )
        
        # Create weather service and get variable data
        weather_service = WeatherService(store_path)
        variable_data = weather_service.get_variable_data(variable_name, max_elements)
        
        if variable_data is None:
            raise HTTPException(
                status_code=404,
                detail=f"Variable '{variable_name}' not found"
            )
        
        return WeatherDataResponse(**variable_data)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )
