from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from pathlib import Path

from models.tree import TreeStructureResponse, NodeDataResponse
from services.tree_service import TreeService
from core.config import settings

router = APIRouter(prefix="/tree", tags=["tree"])

@router.get("/structure", response_model=TreeStructureResponse)
async def get_tree_structure(store_name: str = Query("structure_tree", description="Name of the Zarr store")):
    """
    Get the complete tree structure for a Zarr store.
    
    Args:
        store_name: Name of the store (default: structure_tree)
        
    Returns:
        TreeStructureResponse with the complete hierarchy
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
        
        # Create tree service and build structure
        tree_service = TreeService(store_path)
        nodes = tree_service.build_tree_structure()
        
        if nodes is None:
            raise HTTPException(
                status_code=500,
                detail="Failed to build tree structure"
            )
        
        # Count total nodes recursively
        def count_nodes(node_list):
            count = len(node_list)
            for node in node_list:
                if node.children:
                    count += count_nodes(node.children)
            return count
        
        total_nodes = count_nodes(nodes)
        
        return TreeStructureResponse(
            nodes=nodes,
            store_name=store_name,
            total_nodes=total_nodes
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

@router.get("/node", response_model=NodeDataResponse)
async def get_node_info(
    path: str = Query("", description="Path to the node in the store"),
    store_name: str = Query("structure_tree", description="Name of the Zarr store")
):
    """
    Get detailed information about a specific node.
    
    Args:
        path: Path to the node (empty string for root)
        store_name: Name of the store
        
    Returns:
        NodeDataResponse with node details
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
        
        # Create tree service and get node info
        tree_service = TreeService(store_path)
        node_info = tree_service.get_node_info(path)
        
        if node_info is None:
            raise HTTPException(
                status_code=404,
                detail=f"Node not found: {path}"
            )
        
        return NodeDataResponse(
            id=path or "root",
            path=path,
            type=node_info["type"],
            variables=node_info.get("variables"),
            metadata=node_info.get("metadata"),
            children_count=node_info.get("children_count")
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

@router.get("/file/data")
async def get_file_data(
    path: str = Query(..., description="Path to the file in the store"),
    store_name: str = Query("structure_tree", description="Name of the Zarr store")
):
    """
    Get the actual data from a file (array) in the store.
    
    Args:
        path: Path to the file
        store_name: Name of the store
        
    Returns:
        Dictionary with file data and metadata
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
        
        # Create tree service and get file data
        tree_service = TreeService(store_path)
        file_data = tree_service.get_file_data(path)
        
        if file_data is None:
            raise HTTPException(
                status_code=404,
                detail=f"File not found or not readable: {path}"
            )
        
        return file_data
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

@router.get("/store/summary")
async def get_store_summary(store_name: str = Query("structure_tree", description="Name of the Zarr store")):
    """
    Get a summary of the store.
    
    Args:
        store_name: Name of the store
        
    Returns:
        Dictionary with store summary information
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
        
        # Create tree service and get summary
        tree_service = TreeService(store_path)
        summary = tree_service.get_store_summary()
        
        if "error" in summary:
            raise HTTPException(
                status_code=500,
                detail=summary["error"]
            )
        
        return summary
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )
