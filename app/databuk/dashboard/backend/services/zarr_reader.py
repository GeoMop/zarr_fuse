import zarr
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class ZarrReader:
    """Service for reading Zarr stores and building tree structures."""
    
    def __init__(self, store_path: Path):
        """Initialize with a Zarr store path."""
        self.store_path = store_path
        self._store = None
        self._root_group = None
    
    def open_store(self) -> bool:
        """Open the Zarr store and return success status."""
        try:
            if not self.store_path.exists():
                logger.error(f"Store path does not exist: {self.store_path}")
                return False
            
            self._store = zarr.open(str(self.store_path), mode='r')
            self._root_group = self._store
            logger.info(f"Successfully opened Zarr store: {self.store_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to open Zarr store {self.store_path}: {e}")
            return False
    
    def close_store(self):
        """Close the Zarr store."""
        if self._store is not None:
            try:
                self._store.close()
                self._store = None
                self._root_group = None
                logger.info("Zarr store closed")
            except Exception as e:
                logger.error(f"Error closing store: {e}")
    
    def get_store_info(self) -> Dict[str, Any]:
        """Get basic information about the store."""
        if not self._store:
            return {}
        
        try:
            return {
                "name": self.store_path.name,
                "path": str(self.store_path),
                "type": "zarr",
                "attrs": dict(self._store.attrs) if hasattr(self._store, 'attrs') else {}
            }
        except Exception as e:
            logger.error(f"Error getting store info: {e}")
            return {}
    
    def list_children(self, group_path: str = "") -> List[Tuple[str, str]]:
        """
        List children of a group.
        
        Args:
            group_path: Path to the group (empty string for root)
            
        Returns:
            List of tuples: (name, type) where type is 'group' or 'array'
        """
        if not self._store:
            return []
        
        try:
            if group_path == "":
                group = self._store
            else:
                group = self._store[group_path]
            
            children = []
            for name in group.keys():
                item = group[name]
                if hasattr(item, 'keys'):  # It's a group
                    children.append((name, 'group'))
                else:  # It's an array
                    children.append((name, 'array'))
            
            return children
        except Exception as e:
            logger.error(f"Error listing children for {group_path}: {e}")
            return []
    
    def get_array_info(self, array_path: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific array."""
        if not self._store:
            return None
        
        try:
            array = self._store[array_path]
            if not hasattr(array, 'shape'):  # Not an array
                return None
            
            return {
                "path": array_path,
                "shape": array.shape,
                "dtype": str(array.dtype),
                "attrs": dict(array.attrs) if hasattr(array, 'attrs') else {}
            }
        except Exception as e:
            logger.error(f"Error getting array info for {array_path}: {e}")
            return None
    
    def get_group_info(self, group_path: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific group."""
        if not self._store:
            return None
        
        try:
            group = self._store[group_path]
            if not hasattr(group, 'keys'):  # Not a group
                return None
            
            children = self.list_children(group_path)
            
            return {
                "path": group_path,
                "children_count": len(children),
                "attrs": dict(group.attrs) if hasattr(group, 'attrs') else {}
            }
        except Exception as e:
            logger.error(f"Error getting group info for {group_path}: {e}")
            return None
    
    def __enter__(self):
        """Context manager entry."""
        self.open_store()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_store()
