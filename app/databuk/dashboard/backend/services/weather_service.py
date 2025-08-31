from pathlib import Path
from typing import List, Dict, Any, Optional
import zarr
import numpy as np
from models.weather import WeatherVariable
import logging

logger = logging.getLogger(__name__)

class WeatherService:
    """Service for reading weather data from Zarr stores."""
    
    def __init__(self, store_path: Path):
        """Initialize with a Zarr store path."""
        self.store_path = store_path
    
    def _get_array_info(self, array_path: str) -> Optional[WeatherVariable]:
        """Get information about a specific array."""
        try:
            full_path = self.store_path / array_path
            if not full_path.exists():
                return None
            
            # Open the array
            array = zarr.open_array(str(full_path))
            
            # Get array attributes
            attrs = array.attrs.asdict()
            
            # Extract shape and other info
            shape = list(array.shape)
            data_type = str(array.dtype)
            
            # Get chunk shape if available
            chunk_shape = None
            if hasattr(array, 'chunks'):
                chunk_shape = list(array.chunks)
            
            # Extract unit and description from attributes
            unit = attrs.get('unit')
            description = attrs.get('description')
            
            # Get coordinate names if available
            coordinates = None
            if 'dimension_names' in attrs:
                coordinates = attrs['dimension_names']
            
            return WeatherVariable(
                name=array_path.split('/')[-1],
                type="array",
                shape=shape,
                unit=unit,
                description=description,
                coordinates=coordinates,
                data_type=data_type,
                chunk_shape=chunk_shape
            )
            
        except Exception as e:
            logger.error(f"Error getting array info for {array_path}: {e}")
            return None
    
    def get_weather_structure(self) -> Optional[List[WeatherVariable]]:
        """
        Get the structure of weather variables in the store.
        
        Returns:
            List of WeatherVariable objects, or None if failed
        """
        try:
            # Open the store
            store = zarr.open_group(str(self.store_path))
            
            variables = []
            
            # Iterate through all items in the store using keys()
            for name in store.keys():
                item = store[name]
                if hasattr(item, 'shape'):  # It's an array
                    var_info = self._get_array_info(name)
                    if var_info:
                        variables.append(var_info)
            
            return variables
            
        except Exception as e:
            logger.error(f"Error getting weather structure: {e}")
            return None
    
    def get_variable_data(self, variable_name: str, max_elements: int = 100) -> Optional[Dict[str, Any]]:
        """
        Get actual data for a specific variable.
        
        Args:
            variable_name: Name of the variable
            max_elements: Maximum number of elements to return
            
        Returns:
            Dictionary with variable data, or None if failed
        """
        try:
            full_path = self.store_path / variable_name
            if not full_path.exists():
                return None
            
            # Open the array
            array = zarr.open_array(str(full_path))
            
            # Get array data (limit size for performance)
            data = array[:]
            
            # Convert to list and limit size
            if data.size > max_elements:
                # Take a sample if too large
                data = data.flatten()[:max_elements]
            
            data_list = data.tolist()
            
            # Get attributes
            attrs = array.attrs.asdict()
            
            return {
                "name": variable_name,
                "data": data_list,
                "shape": list(array.shape),
                "unit": attrs.get('unit'),
                "data_type": str(array.dtype)
            }
            
        except Exception as e:
            logger.error(f"Error getting variable data for {variable_name}: {e}")
            return None
