from typing import List, Literal, Optional
from pydantic import BaseModel, Field

class TreeNode(BaseModel):
    """Represents a node in the tree structure."""
    id: str = Field(..., description="Unique identifier for the node (usually the path)")
    name: str = Field(..., description="Display name of the node")
    type: Literal["folder", "file"] = Field(..., description="Type of the node")
    children: Optional[List["TreeNode"]] = Field(None, description="Child nodes if this is a folder")
    path: str = Field(..., description="Full path to the node in the store")
    
    class Config:
        # Allow recursive models
        from_attributes = True

class TreeStructureResponse(BaseModel):
    """Response model for tree structure endpoint."""
    nodes: List[TreeNode] = Field(..., description="Root level nodes")
    store_name: str = Field(..., description="Name of the Zarr store")
    total_nodes: int = Field(..., description="Total number of nodes in the tree")

class NodeDataResponse(BaseModel):
    """Response model for individual node data."""
    id: str = Field(..., description="Node identifier")
    path: str = Field(..., description="Full path to the node")
    type: Literal["folder", "file"] = Field(..., description="Type of the node")
    variables: Optional[List[str]] = Field(None, description="Available variables if file")
    metadata: Optional[dict] = Field(None, description="Additional metadata")
    children_count: Optional[int] = Field(None, description="Number of children if folder")

# Update forward references
TreeNode.model_rebuild()
