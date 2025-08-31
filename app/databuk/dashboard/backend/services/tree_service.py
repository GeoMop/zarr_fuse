from pathlib import Path
from typing import List, Dict, Any, Optional
from models.tree import TreeNode
from services.zarr_reader import ZarrReader
import logging

logger = logging.getLogger(__name__)

class TreeService:
    """Service for building tree structures from Zarr stores."""
    
    def __init__(self, store_path: Path):
        """Initialize with a Zarr store path."""
        self.store_path = store_path
        self._node_counter = 0
    
    def _generate_node_id(self, path: str) -> str:
        """Generate a unique node ID."""
        self._node_counter += 1
        return f"node_{self._node_counter}_{path.replace('/', '_')}"
    
    def _get_node_name(self, path: str) -> str:
        """Extract the display name from a path."""
        if path == "":
            return "root"
        return path.split("/")[-1]
    
    def _build_tree_recursive(self, reader: ZarrReader, group_path: str = "") -> List[TreeNode]:
        """
        Recursively build the tree structure for a group.
        
        Args:
            reader: ZarrReader instance
            group_path: Path to the current group
            
        Returns:
            List of TreeNode objects
        """
        try:
            children = reader.list_children(group_path)
            nodes = []
            
            for name, item_type in children:
                current_path = f"{group_path}/{name}" if group_path else name
                node_id = self._generate_node_id(current_path)
                
                if item_type == 'group':
                    # Recursively get children for this group
                    child_nodes = self._build_tree_recursive(reader, current_path)
                    
                    node = TreeNode(
                        id=node_id,
                        name=name,
                        type="folder",
                        path=current_path,
                        children=child_nodes
                    )
                else:  # item_type == 'array'
                    node = TreeNode(
                        id=node_id,
                        name=name,
                        type="file",
                        path=current_path,
                        children=None
                    )
                
                nodes.append(node)
            
            return nodes
        except Exception as e:
            logger.error(f"Error building tree for {group_path}: {e}")
            return []
    
    def build_tree_structure(self) -> Optional[List[TreeNode]]:
        """
        Build the complete tree structure for the store.
        
        Returns:
            List of root-level TreeNode objects, or None if failed
        """
        try:
            with ZarrReader(self.store_path) as reader:
                if not reader.open_store():
                    logger.error(f"Failed to open store: {self.store_path}")
                    return None
                
                # Build the tree starting from root
                root_nodes = self._build_tree_recursive(reader, "")
                
                # If we have a single root group, return its children
                # Otherwise, return the root nodes as-is
                if len(root_nodes) == 1 and root_nodes[0].type == "folder":
                    return root_nodes[0].children or []
                
                return root_nodes
                
        except Exception as e:
            logger.error(f"Error building tree structure: {e}")
            return None
    
    def get_node_info(self, node_path: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a specific node.
        
        Args:
            node_path: Path to the node
            
        Returns:
            Dictionary with node information, or None if failed
        """
        try:
            with ZarrReader(self.store_path) as reader:
                if not reader.open_store():
                    return None
                
                # Check if it's a group or array
                children = reader.list_children(node_path)
                if children:  # It's a group
                    group_info = reader.get_group_info(node_path)
                    if group_info:
                        return {
                            "type": "folder",
                            "path": node_path,
                            "children_count": len(children),
                            "children": [{"name": name, "type": item_type} for name, item_type in children],
                            "metadata": group_info.get("attrs", {})
                        }
                else:  # It's an array
                    array_info = reader.get_array_info(node_path)
                    if array_info:
                        return {
                            "type": "file",
                            "path": node_path,
                            "variables": [node_path.split("/")[-1]],
                            "metadata": array_info
                        }
                
                return None
                
        except Exception as e:
            logger.error(f"Error getting node info for {node_path}: {e}")
            return None
    
    def get_file_data(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        Get the actual data from a file (array) in the store.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary with file data and metadata, or None if failed
        """
        try:
            with ZarrReader(self.store_path) as reader:
                if not reader.open_store():
                    return None
                
                # Get array info first
                array_info = reader.get_array_info(file_path)
                if not array_info:
                    return None
                
                # Read the actual data
                try:
                    import zarr
                    import numpy as np
                    
                    # Open the specific array
                    array = zarr.open(str(self.store_path / file_path), mode='r')
                    
                    # Get the data (for small arrays, get all; for large ones, get sample)
                    if array.size <= 1000:  # Small array, get all data
                        data = array[:]
                    else:  # Large array, get sample
                        # Get first 1000 elements or first dimension if multi-dimensional
                        if len(array.shape) == 1:
                            data = array[:1000]
                        else:
                            # For multi-dimensional, get first slice
                            indices = [slice(0, min(1000, dim)) for dim in array.shape]
                            data = array[tuple(indices)]
                    
                    return {
                        "path": file_path,
                        "name": file_path.split("/")[-1],
                        "type": "array",
                        "shape": array.shape,
                        "dtype": str(array.dtype),
                        "size": array.size,
                        "data": data.tolist() if hasattr(data, 'tolist') else data,
                        "metadata": array_info.get("attrs", {}),
                        "sample_size": len(data) if hasattr(data, '__len__') else 1
                    }
                    
                except Exception as e:
                    logger.error(f"Error reading array data for {file_path}: {e}")
                    # Return metadata only if data reading fails
                    return {
                        "path": file_path,
                        "name": file_path.split("/")[-1],
                        "type": "array",
                        "error": f"Failed to read data: {str(e)}",
                        "metadata": array_info
                    }
                
        except Exception as e:
            logger.error(f"Error getting file data for {file_path}: {e}")
            return None
    
    def get_store_summary(self) -> Dict[str, Any]:
        """Get a summary of the store."""
        try:
            with ZarrReader(self.store_path) as reader:
                if not reader.open_store():
                    return {"error": "Failed to open store"}
                
                store_info = reader.get_store_info()
                root_children = reader.list_children("")
                
                return {
                    "name": store_info.get("name", "Unknown"),
                    "path": str(self.store_path),
                    "root_items": len(root_children),
                    "type": "zarr",
                    "status": "active"
                }
                
        except Exception as e:
            logger.error(f"Error getting store summary: {e}")
            return {"error": str(e)}
