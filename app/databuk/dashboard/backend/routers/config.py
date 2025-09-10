from fastapi import APIRouter, HTTPException
from core.config_manager import config_manager, EndpointConfig
from typing import Dict, Any

router = APIRouter(prefix="/config", tags=["configuration"])

@router.get("/endpoints")
async def get_endpoints() -> Dict[str, Any]:
    """Get all configured endpoints"""
    try:
        endpoints = config_manager.get_endpoints()
        return {
            "status": "success",
            "endpoints": {name: endpoint.dict() for name, endpoint in endpoints.items()}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load endpoints: {str(e)}")

@router.get("/endpoints/{endpoint_name}")
async def get_endpoint(endpoint_name: str) -> Dict[str, Any]:
    """Get specific endpoint configuration"""
    try:
        endpoint = config_manager.get_endpoint(endpoint_name)
        if endpoint is None:
            raise HTTPException(status_code=404, detail=f"Endpoint '{endpoint_name}' not found")
        
        return {
            "status": "success",
            "endpoint": endpoint.dict()
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load endpoint: {str(e)}")

@router.get("/current")
async def get_current_endpoint() -> Dict[str, Any]:
    """Get the current (first) endpoint configuration"""
    try:
        endpoint = config_manager.get_first_endpoint()
        if endpoint is None:
            raise HTTPException(status_code=404, detail="No endpoints configured")
        
        return {
            "status": "success",
            "endpoint": endpoint.dict()
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load current endpoint: {str(e)}")

@router.post("/reload")
async def reload_config() -> Dict[str, Any]:
    """Reload configuration from file"""
    try:
        config_manager.reload_config()
        return {
            "status": "success",
            "message": "Configuration reloaded successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to reload configuration: {str(e)}")
