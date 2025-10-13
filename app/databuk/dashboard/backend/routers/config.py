from fastapi import APIRouter, HTTPException
from core.config_manager import load_endpoints, get_first_endpoint, EndpointConfig
from typing import Dict, Any

router = APIRouter(prefix="/config", tags=["configuration"])

@router.get("/endpoints")
async def get_endpoints() -> Dict[str, Any]:
    """Get all configured endpoints"""
    try:
        endpoints = load_endpoints()
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
        endpoints = load_endpoints()
        endpoint = endpoints.get(endpoint_name)
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
        endpoint = get_first_endpoint()
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
    """Reload configuration from file (pure function approach - always fresh)"""
    try:
        # Pure function approach - always loads fresh from file
        endpoints = load_endpoints()
        return {
            "status": "success",
            "message": "Configuration reloaded successfully",
            "endpoints_count": len(endpoints)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to reload configuration: {str(e)}")
