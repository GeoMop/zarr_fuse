from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any, Optional
from services.s3_service import s3_service
from core.config_manager import config_manager

router = APIRouter(prefix="/s3", tags=["s3"])

@router.get("/connect")
async def connect_to_s3():
    """Connect to S3 using the current configuration"""
    try:
        endpoint_config = config_manager.get_first_endpoint()
        if not endpoint_config:
            raise HTTPException(status_code=400, detail="No endpoint configuration found")
        
        success = s3_service.connect(endpoint_config)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to connect to S3")
        
        return {"status": "success", "message": "Connected to S3 successfully"}
    
    except Exception as e:
        print(f"Connection error details: {str(e)}")  # Debug print
        raise HTTPException(status_code=500, detail=f"Connection error: {str(e)}")

@router.get("/structure")
async def get_store_structure():
    """Get the structure of the Zarr store with sample data"""
    try:
        # Ensure we're connected
        if not s3_service._fs:
            # Try to connect first
            endpoint_config = config_manager.get_first_endpoint()
            if not endpoint_config:
                raise HTTPException(status_code=400, detail="No endpoint configuration found")
            
            success = s3_service.connect(endpoint_config)
            if not success:
                raise HTTPException(status_code=500, detail="Failed to connect to S3")
        
        structure = s3_service.get_store_structure()
        return {"status": "success", "structure": structure}
    
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=f"Store not found: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get store structure: {str(e)}")

@router.get("/array/{array_path:path}")
async def get_array_data(array_path: str, include_sample: bool = False):
    """Get data from a specific array"""
    try:
        # Ensure we're connected
        if not s3_service._fs:
            endpoint_config = config_manager.get_first_endpoint()
            if not endpoint_config:
                raise HTTPException(status_code=400, detail="No endpoint configuration found")
            
            success = s3_service.connect(endpoint_config)
            if not success:
                raise HTTPException(status_code=500, detail="Failed to connect to S3")
        
        slice_info = {"include_sample": include_sample} if include_sample else None
        data = s3_service.get_array_data(array_path, slice_info)
        return {"status": "success", "data": data}
    
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=f"Array not found: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get array data: {str(e)}")

@router.get("/node/{store_name}/{node_path:path}")
async def get_node_details(store_name: str, node_path: str):
    """Get detailed information about a specific node in a Zarr store"""
    try:
        # Ensure we're connected
        if not s3_service._fs:
            endpoint_config = config_manager.get_first_endpoint()
            if not endpoint_config:
                raise HTTPException(status_code=400, detail="No endpoint configuration found")
            
            success = s3_service.connect(endpoint_config)
            if not success:
                raise HTTPException(status_code=500, detail="Failed to connect to S3")
        
        node_details = s3_service.get_node_details(store_name, node_path)
        return {"status": "success", "node_details": node_details}
    
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=f"Node not found: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get node details: {str(e)}")

@router.get("/status")
async def get_s3_status():
    """Get the current S3 connection status"""
    try:
        is_connected = s3_service._fs is not None
        current_config = s3_service._current_config
        
        status = {
            "connected": is_connected,
            "endpoint": current_config.STORE_URL if current_config else None,
            "description": current_config.Description if current_config else None
        }
        
        return {"status": "success", "s3_status": status}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get S3 status: {str(e)}")
 



 