import zarr
from zarr.storage import FsspecStore
import logging
import math
import os
from typing import Dict, Any, Optional, List
from core.config_manager import load_endpoints, get_first_endpoint, EndpointConfig
import fsspec

# Import zarr_fuse (now installed via pip)
import zarr_fuse

logger = logging.getLogger(__name__)

def clean_nan_values(data):
    """Clean NaN and Infinity values from data for JSON serialization"""
    if isinstance(data, float):
        if math.isnan(data):
            return "NaN"  # Return as string
        elif math.isinf(data):
            return "Infinity" if data > 0 else "-Infinity"
    elif isinstance(data, dict):
        return {k: clean_nan_values(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [clean_nan_values(item) for item in data]
    return data

class S3Service:
    """Service for S3 operations with custom S3 configuration"""
    
    def __init__(self):
        self._fs: Optional[fsspec.AbstractFileSystem] = None
        self._current_config: Optional[EndpointConfig] = None
    
    def _get_storage_options(self, config: Optional[EndpointConfig] = None) -> Dict[str, Any]:
        """Get S3 storage options - centralized configuration"""
        if not config and self._current_config:
            config = self._current_config
        
        if not config:
            raise ValueError("No configuration provided")
            
        return {
            'key': os.getenv('ZF_S3_ACCESS_KEY'),
            'secret': os.getenv('ZF_S3_SECRET_KEY'),
            'client_kwargs': {'endpoint_url': os.getenv('ZF_S3_ENDPOINT_URL')},
            'config_kwargs': {
                's3': {
                    'payload_signing_enabled': False,
                    'addressing_style': 'path'
                },
                'retries': {'max_attempts': 5, 'mode': 'standard'},
                'connect_timeout': 20,
                'read_timeout': 60,
                'request_checksum_calculation': 'when_required',
                'response_checksum_validation': 'when_required'
            }
        }
    
    def _open_zarr_store(self) -> tuple:
        """Open Zarr store using zarr_fuse.open_store and deserialized schema from endpoint config."""
        import os
        schema_file = self._current_config.schema_file
        # If schema_file is a relative path, resolve it relative to backend directory
        if not os.path.isabs(schema_file):
            schema_path = os.path.join(os.path.dirname(__file__), '..', schema_file)
        else:
            schema_path = schema_file
        print(f"Attempting to open store with zarr_fuse using schema: {schema_path}")
        import yaml
        with open(schema_path, 'r', encoding='utf-8') as f:
            raw_dict = yaml.safe_load(f)
            from zarr_fuse.zarr_schema import SchemaAddress
            address = SchemaAddress(addr=[], file=self._current_config.store_url)
            schema = zarr_fuse.zarr_schema.dict_deserialize(raw_dict, address)
        kwargs = {"S3_ENDPOINT_URL": os.getenv("ZF_S3_ENDPOINT_URL")}
        node = zarr_fuse.open_store(schema, **kwargs)
        return None, node
    
    def _find_group_variables(self, store, store_group, group_path: str = "") -> List[str]:
        """Find available variables in a group using multiple fallback approaches"""
        
        # Method 1: Use store.list_dir() (preferred for FsspecStore)
        try:
            store_keys = list(store.list_dir(""))
            print(f"Store list_dir result: {len(store_keys)} items")
            
            group_variables = []
            group_prefix = f"{group_path}/" if group_path else ""
            
            for key in store_keys:
                if key.startswith(group_prefix) and key.endswith('/zarr.json'):
                    var_path_parts = key.split('/')
                    # Extract variable name from group_path/variable_name/zarr.json
                    expected_parts = len(group_prefix.split('/')) + 1 if group_prefix else 2
                    if len(var_path_parts) == expected_parts:
                        variable_name = var_path_parts[-2]  # Second to last part
                        group_variables.append(variable_name)
            
            if group_variables:
                print(f"SUCCESS: Found {len(group_variables)} variables using list_dir in {group_path or 'root'}")
                return group_variables
        except Exception as e:
            print(f"WARNING: list_dir method failed: {e}")
        
        # Method 2: Access group directly
        try:
            if group_path:
                target_group = store_group[group_path]
            else:
                target_group = store_group
            group_variables = list(target_group.keys())
            print(f"SUCCESS: Found {len(group_variables)} variables using group keys in {group_path or 'root'}")
            return group_variables
        except Exception as e:
            print(f"WARNING: Group access method failed: {e}")
        
        # Both methods failed
        print(f"ERROR: Could not find variables in {group_path or 'root'} using any method")
        return []
    
    def _process_group(self, group, group_path: str) -> Dict[str, Any]:
        """Recursively process a Zarr group"""
        result = {
            'name': group_path.split('/')[-1] if group_path else 'root',
            'path': group_path,
            'type': 'group',
            'children': []
        }
        
        # Use test script approach for subgroups and arrays
        all_subkeys = list(group.keys()) if hasattr(group, 'keys') else []
        subgroups = []
        arrays = []
        
        # Check if this is a group that needs store-level variable detection
        if hasattr(group, 'store') and hasattr(group, 'path'):
            group_path_attr = getattr(group, 'path', '')
            # Generic check for any group that might have variables in store
            if group_path_attr:
                try:
                    store_keys = list(group.store.list_dir(""))
                    group_prefix = f"{group_path_attr}/"
                    group_keys = [key for key in store_keys if key.startswith(group_prefix) and '/' in key[len(group_prefix):]]
                    # Extract variable names from group_path/variable_name/...
                    variable_names = set()
                    for key in group_keys:
                        parts = key.split('/')
                        if len(parts) >= 2 and parts[0] == group_path_attr:
                            var_name = parts[1]
                            if var_name and not var_name.startswith('.'):
                                variable_names.add(var_name)
                    
                    all_subkeys = list(variable_names)
                    
                    # Assume all are arrays (weather variables)
                    arrays = all_subkeys
                    subgroups = []
                    
                    # Process arrays
                    for name in arrays:
                        try:
                            array_info = {
                                'name': name,
                                'path': f"{group_path}/{name}" if group_path else name,
                                'type': 'array',
                                'shape': 'unknown',
                                'dtype': 'unknown'
                            }
                            result['children'].append(array_info)
                        except Exception as e:
                            print(f"WARNING: Could not process array {name}: {e}")
                    
                    return result
                    
                except Exception as e:
                    print(f"WARNING: Store-level variable detection failed for {group_path_attr}: {e}")
        
        for key in all_subkeys:
            try:
                item = group[key]
                if hasattr(item, 'shape'):  # It's an array
                    arrays.append(key)
                else:  # It's a group
                    subgroups.append(key)
            except Exception as e:
                print(f"WARNING: Could not access subkey {key}: {e}")
        
        # Process arrays first
        for name in arrays:
            try:
                dataset = group[name]
                sample_data = self._get_sample_data(dataset)
                result['children'].append({
                    'name': name,
                    'path': f"{group_path}/{name}" if group_path else name,
                    'type': 'array',
                    'shape': getattr(dataset, 'shape', None),
                    'dtype': str(getattr(dataset, 'dtype', 'unknown')),
                    'chunks': getattr(dataset, 'chunks', None),
                    'size': getattr(dataset, 'size', None),
                    'sample_data': sample_data
                })
            except Exception as e:
                print(f"WARNING: Failed to process array {name}: {e}")

        # Then process subgroups
        for name in subgroups:
            try:
                subgroup = group[name]
                subgroup_result = self._process_group(subgroup, f"{group_path}/{name}" if group_path else name)
                result['children'].append(subgroup_result)
            except Exception as e:
                print(f"WARNING: Failed to process subgroup {name}: {e}")
        
        return result
    
    def connect(self, endpoint_config: Optional[EndpointConfig] = None) -> bool:
        """Connect to S3 using common S3 configuration"""
        try:
            # Use first endpoint if none provided
            if endpoint_config is None:
                endpoint_config = get_first_endpoint()
                if not endpoint_config:
                    raise ValueError("No endpoint configuration found")
            
            # Use fsspec.get_mapper approach for listing
            storage_options = self._get_storage_options(endpoint_config)
            
            # Use fsspec.filesystem for listing operations
            self._fs = fsspec.filesystem('s3', **storage_options)
            self._current_config = endpoint_config
            logger.info(f"Successfully connected to S3: {endpoint_config.store_url}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to S3: {e}")
            print(f"Connection error details: {str(e)}")
            return False
    
    def get_store_structure(self) -> Dict[str, Any]:
        """Get the structure of the specific Zarr store from STORE_URL"""
        if not self._fs or not self._current_config:
            raise ValueError("Not connected to S3 or missing endpoint config")
        
        # Always use zarr_fuse path
        print(f"Using zarr_fuse path for structure")
        return self._get_store_structure_zarr_fuse()
        
        # LEGACY CODE BELOW - COMMENTED OUT (kept for reference)
        try:
            # Extract store path from store_url
            store_url = self._current_config.store_url
            if store_url.startswith('s3://'):
                store_path = store_url[5:]  # Remove 's3://' prefix
            else:
                store_path = store_url
            
            print(f"Using specific store path from store_url: {store_path}")
            
            # Get store name (last part of path)
            store_name = store_path.split('/')[-1]
            print(f"Store name: {store_name}")
            
            # Try to open the specific Zarr store
            zarr_stores = []
            try:
                store_structure = self._extract_structure(store_path)
                zarr_stores.append({
                    'name': store_name,
                    'path': store_path,
                    'type': 'zarr_store',
                    'structure': store_structure
                })
                print(f"SUCCESS: Successfully processed: {store_name}")
            except Exception as e:
                # If we can't read the store, still list it but mark as error
                zarr_stores.append({
                    'name': store_name,
                    'path': store_path,
                    'type': 'zarr_store',
                    'error': f"Could not read store: {str(e)}"
                })
                print(f"ERROR: Error processing: {store_name} - {e}")
            
            print(f"Total Zarr stores found: {len(zarr_stores)}")
            
            # Debug: print the structure of the store
            if zarr_stores and zarr_stores[0].get('structure'):
                print("Store structure:")
                import json
                print(json.dumps(zarr_stores[0]['structure'], indent=2, default=str))
            
            # Extract bucket name for response
            bucket_name = store_path.split('/')[0]
            
            return {
                'status': 'success',
                'bucket_name': bucket_name,
                'store_url': store_url,
                'total_stores': len(zarr_stores),
                'stores': zarr_stores
            }
            
        except Exception as e:
            logger.error(f"Failed to get store structure: {e}")
            raise ValueError(f"Failed to get store structure: {e}")
    
    def _get_store_structure_zarr_fuse(self) -> Dict[str, Any]:
        """Extract structure using zarr_fuse node only."""
        try:
            store_url = self._current_config.store_url
            store_name = store_url.split('/')[-1]
            print(f"Opening store with zarr_fuse: {store_url}")
            # Open with zarr_fuse
            _, node = self._open_zarr_store()
            # Extract structure from node
            # Assumption: node.to_dict() returns the expected structure
            def node_to_legacy(node, path=""):
                # Her node için legacy children dizisini doldur
                children = []
                # Variables (arrays)
                if hasattr(node, 'dataset') and node.dataset:
                    ds = node.dataset
                    if hasattr(ds, 'data_vars'):
                        for var_name, var in ds.data_vars.items():
                            children.append({
                                'name': var_name,
                                'path': f"{path}/{var_name}" if path else var_name,
                                'type': 'array',
                                'shape': list(var.shape),
                                'dtype': str(var.dtype),
                                'chunks': getattr(var, 'chunks', None),
                                'size': getattr(var, 'size', None),
                                'sample_data': []
                            })
                # Subgroups (children)
                if hasattr(node, 'children') and node.children:
                    for child_name, child_node in node.children.items():
                        children.append(node_to_legacy(child_node, f"{path}/{child_name}" if path else child_name))
                return {
                    'name': node.name if hasattr(node, 'name') else 'root',
                    'path': path,
                    'type': 'group',
                    'children': children
                }

            legacy_structure = node_to_legacy(node)
            print(f"DEBUG: legacy_structure = {legacy_structure}")
            zarr_stores = [{
                'name': store_name,
                'path': store_url,
                'type': 'zarr_store',
                'structure': legacy_structure
            }]
            bucket_name = store_url.split('/')[2] if store_url.startswith('s3://') else store_url.split('/')[0]
            return {
                'status': 'success',
                'bucket_name': bucket_name,
                'store_url': store_url,
                'total_stores': len(zarr_stores),
                'stores': zarr_stores
            }
        except Exception as e:
            import traceback
            logger.error(f"Failed to get store structure with zarr_fuse: {e}")
            traceback.print_exc()
            raise ValueError(f"Failed to get store structure with zarr_fuse: {e}")
    
    def _build_structure_from_xarray(self, ds) -> Dict[str, Any]:
        """Build structure from xarray dataset"""
        structure = {
            'vars': {},
            'coords': {},
            'dims': list(ds.dims.keys()),
            'attrs': dict(ds.attrs)
        }
        
        # Add data variables
        for var_name, var in ds.data_vars.items():
            structure['vars'][var_name] = {
                'dims': list(var.dims),
                'shape': var.shape,
                'dtype': str(var.dtype),
                'attrs': dict(var.attrs)
            }
        
        # Add coordinates
        for coord_name, coord in ds.coords.items():
            structure['coords'][coord_name] = {
                'dims': list(coord.dims),
                'shape': coord.shape,
                'dtype': str(coord.dtype),
                'attrs': dict(coord.attrs)
            }
        
        return structure
    
    def _build_structure_from_zarr_group(self, zarr_group, group_path: str = "") -> Dict[str, Any]:
        """Build structure from zarr group"""
        structure = {
            'vars': {},
            'coords': {},
            'dims': {},
            'attrs': dict(zarr_group.attrs),
            'groups': {}
        }
        
        # Add arrays (variables)
        for name, array in zarr_group.arrays():
            structure['vars'][name] = {
                'shape': array.shape,
                'dtype': str(array.dtype),
                'chunks': array.chunks,
                'attrs': dict(array.attrs)
            }
        
        # Add groups
        for name, group in zarr_group.groups():
            structure['groups'][name] = self._build_structure_from_zarr_group(group, f"{group_path}/{name}")
        
        return structure
    
    def _build_structure_from_node(self, node: 'zarr_fuse.Node') -> Dict[str, Any]:
        """Build structure dictionary from zarr_fuse Node"""
        structure = {}
        
        # Add current node's dataset info
        try:
            ds = node.dataset
            if ds:
                # Add variables
                if ds.data_vars:
                    structure['vars'] = {}
                    for var_name, var in ds.data_vars.items():
                        structure['vars'][var_name] = {
                            'dims': list(var.dims),
                            'shape': list(var.shape),
                            'dtype': str(var.dtype),
                            'attrs': dict(var.attrs)
                        }
                
                # Add coordinates
                if ds.coords:
                    structure['coords'] = {}
                    for coord_name, coord in ds.coords.items():
                        structure['coords'][coord_name] = {
                            'dims': list(coord.dims),
                            'shape': list(coord.shape),
                            'dtype': str(coord.dtype),
                            'attrs': dict(coord.attrs)
                        }
                
                # Add global attributes
                if ds.attrs:
                    structure['attrs'] = dict(ds.attrs)
        except Exception as e:
            print(f"Warning: Could not read dataset for node {node.name}: {e}")
        
        # Add children (subgroups)
        if hasattr(node, 'children') and node.children:
            structure['groups'] = {}
            for child_name, child_node in node.children.items():
                structure['groups'][child_name] = self._build_structure_from_node(child_node)
        
        return structure
    
    def _build_structure_from_dataset(self, ds: 'xr.Dataset', group_path: str) -> Dict[str, Any]:
        """Build structure dictionary from xarray Dataset"""
        structure = {}
        
        # Add variables
        if ds.data_vars:
            structure['vars'] = {}
            for var_name, var in ds.data_vars.items():
                structure['vars'][var_name] = {
                    'dims': list(var.dims),
                    'shape': list(var.shape),
                    'dtype': str(var.dtype),
                    'attrs': dict(var.attrs)
                }
        
        # Add coordinates
        if ds.coords:
            structure['coords'] = {}
            for coord_name, coord in ds.coords.items():
                structure['coords'][coord_name] = {
                    'dims': list(coord.dims),
                    'shape': list(coord.shape),
                    'dtype': str(coord.dtype),
                    'attrs': dict(coord.attrs)
                }
        
        # Add global attributes
        if ds.attrs:
            structure['attrs'] = dict(ds.attrs)
        
        return structure
    
    def _build_structure_from_zarr_group(self, group: 'zarr.Group', group_path: str) -> Dict[str, Any]:
        """Build structure dictionary from zarr Group"""
        structure = {}
        
        # Add arrays (variables)
        arrays = list(group.arrays())
        if arrays:
            structure['vars'] = {}
            for name, array in arrays:
                structure['vars'][name] = {
                    'dims': list(array.shape),  # Simplified - zarr doesn't have dim names
                    'shape': list(array.shape),
                    'dtype': str(array.dtype),
                    'attrs': dict(array.attrs)
                }
        
        # Add subgroups
        subgroups = list(group.groups())
        if subgroups:
            structure['groups'] = {}
            for name, subgroup in subgroups:
                structure['groups'][name] = self._build_structure_from_zarr_group(subgroup, f"{group_path}/{name}")
        
        # Add group attributes
        if group.attrs:
            structure['attrs'] = dict(group.attrs)
        
        return structure
    
    def _convert_to_legacy_format(self, zf_structure: Dict[str, Any]) -> Dict[str, Any]:
        """Convert zarr_fuse structure format to legacy dashboard format"""
        legacy = {
            'name': 'root',
            'path': '',
            'type': 'group',
            'children': []
        }
        
        # Add variables as arrays
        if 'vars' in zf_structure:
            for var_name, var_info in zf_structure['vars'].items():
                legacy['children'].append({
                    'name': var_name,
                    'path': var_name,
                    'type': 'array',
                    'shape': var_info['shape'],
                    'dtype': var_info['dtype'],
                    'chunks': var_info['shape'],  # Simplified
                    'size': var_info['shape'][0] if var_info['shape'] else 0,
                    'sample_data': []  # Will be filled by legacy code
                })
        
        # Add groups recursively
        if 'groups' in zf_structure:
            for group_name, group_info in zf_structure['groups'].items():
                group_legacy = {
                    'name': group_name,
                    'path': group_name,
                    'type': 'group',
                    'children': []
                }
                
                # Add group variables
                if 'vars' in group_info:
                    for var_name, var_info in group_info['vars'].items():
                        group_legacy['children'].append({
                            'name': var_name,
                            'path': f"{group_name}/{var_name}",
                            'type': 'array',
                            'shape': var_info['shape'],
                            'dtype': var_info['dtype'],
                            'chunks': var_info['shape'],
                            'size': var_info['shape'][0] if var_info['shape'] else 0,
                            'sample_data': []
                        })
                
                # Add subgroups recursively
                if 'groups' in group_info:
                    for subgroup_name, subgroup_info in group_info['groups'].items():
                        subgroup_legacy = self._convert_group_to_legacy(subgroup_name, f"{group_name}/{subgroup_name}", subgroup_info)
                        group_legacy['children'].append(subgroup_legacy)
                
                legacy['children'].append(group_legacy)
        
        return legacy
    
    def _convert_group_to_legacy(self, group_name: str, group_path: str, group_info: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a single group to legacy format"""
        group_legacy = {
            'name': group_name,
            'path': group_path,
            'type': 'group',
            'children': []
        }
        
        # Add variables
        if 'vars' in group_info:
            for var_name, var_info in group_info['vars'].items():
                group_legacy['children'].append({
                    'name': var_name,
                    'path': f"{group_path}/{var_name}",
                    'type': 'array',
                    'shape': var_info['shape'],
                    'dtype': var_info['dtype'],
                    'chunks': var_info['shape'],
                    'size': var_info['shape'][0] if var_info['shape'] else 0,
                    'sample_data': []
                })
        
        # Add subgroups recursively
        if 'groups' in group_info:
            for subgroup_name, subgroup_info in group_info['groups'].items():
                subgroup_legacy = self._convert_group_to_legacy(subgroup_name, f"{group_path}/{subgroup_name}", subgroup_info)
                group_legacy['children'].append(subgroup_legacy)
        
        return group_legacy
    
    def _extract_structure(self, store_path: str) -> Dict[str, Any]:
        """Extract structure from a Zarr store - refactored and simplified"""
        try:
            print(f"Extracting structure from: {store_path}")
            
            # Step 1: Open the Zarr store
            store, store_group = self._open_zarr_store(store_path)
            
            # Handle early return case (minimal structure)
            if isinstance(store_group, dict):
                return store_group
            
            # Step 2: Process the store group recursively
            final_result = self._process_group(store_group, "")
            print(f"Final structure: {final_result['name']} with {len(final_result['children'])} children")
            return final_result
            
        except Exception as e:
            logger.error(f"Failed to extract structure from {store_path}: {e}")
            raise
    
    def _get_sample_data(self, dataset: zarr.Array) -> List[Any]:
        """Helper to get sample data from a Zarr array."""
        try:
            if dataset.size > 0:
                # Get first 10 values or all if less than 10
                sample_size = min(10, dataset.size)
                if len(dataset.shape) == 1:
                    # 1D array
                    sample_data = dataset[:sample_size]
                elif len(dataset.shape) == 2:
                    # 2D array - get first few rows and columns
                    sample_data = dataset[:min(5, dataset.shape[0]), :min(5, dataset.shape[1])]
                else:
                    # Higher dimensional - get first slice
                    sample_data = dataset[tuple(slice(0, min(3, dim)) for dim in dataset.shape)]
                return clean_nan_values(sample_data.tolist())
            else:
                return []
        except Exception as e:
            logger.warning(f"Failed to get sample data for dataset: {e}")
            return []
    

    def get_node_details(self, store_name: str, node_path: str) -> Dict[str, Any]:
        """Get detailed information about a specific node in a Zarr store"""
        if not self._fs or not self._current_config:
            raise ValueError("Not connected to S3. Call connect() first.")
        
        # TODO: Implement zarr_fuse path for node details
        # For now, use legacy implementation
        print(f"Using legacy path for node details: {node_path}")
        
        '''
        # LEGACY CODE BELOW - NO CHANGES TO EXISTING LOGIC
        try:
            print(f"Getting details for node: {store_name}/{node_path}")
            
            # Use the store path from store_url directly
            store_url = self._current_config.store_url
            if store_url.startswith('s3://'):
                store_path = store_url[5:]  # Remove 's3://' prefix
            else:
                store_path = store_url
            
            print(f"Using store path from store_url: {store_path}")
            
            # Open the store
            storage_options = self._get_storage_options()
            
            # Use zarr_fuse approach: FsspecStore instead of get_mapper
            fs = fsspec.filesystem('s3', asynchronous=False, **storage_options)
            clean_path = store_path  # Already clean, no s3:// prefix
            store = FsspecStore(fs, path=clean_path)
            print(f"Created FsspecStore for node details: {clean_path}")
            
            # Try to open as group first
            try:
                # Remove 'root/' prefix if present
                clean_node_path = node_path.replace('root/', '') if node_path.startswith('root/') else node_path
                
                # Special handling for root node
                if clean_node_path == 'root' or clean_node_path == '':
                    node = zarr.open_group(store, mode='r')
                    node_type = 'group'
                elif clean_node_path:
                    # For FsspecStore, don't use path parameter - create new store with path
                    node_fs = fsspec.filesystem('s3', asynchronous=False, **storage_options)
                    node_store = FsspecStore(node_fs, path=f"{clean_path}/{clean_node_path}")
                    node = zarr.open_group(node_store, mode='r')
                    node_type = 'group'
                else:
                    node = zarr.open_group(store, mode='r')
                    node_type = 'group'
            except Exception as e:
                print(f"WARNING: Failed to open as group: {e}")
                print(f"Now trying to open as array...")
                try:
                    # Remove 'root/' prefix if present
                    clean_node_path = node_path.replace('root/', '') if node_path.startswith('root/') else node_path
                    
                    # Special handling for root node
                    if clean_node_path == 'root' or clean_node_path == '':
                        # Root should be opened as group, not array
                        node = zarr.open_group(store, mode='r')
                        node_type = 'group'
                    elif clean_node_path:
                        # For FsspecStore, don't use path parameter - create new store with path
                        node_fs = fsspec.filesystem('s3', asynchronous=False, **storage_options)
                        node_store = FsspecStore(node_fs, path=f"{clean_path}/{clean_node_path}")
                        node = zarr.open_array(node_store, mode='r')
                        node_type = 'array'
                    else:
                        node = zarr.open_array(store, mode='r')
                        node_type = 'array'
                except Exception as e2:
                    print(f"WARNING: Failed to open as array: {e2}")
                    print(f"ERROR: Both group and array attempts failed")
                    raise FileNotFoundError(f"Node not found: {node_path}")
            
            # Extract node details
            details = {
                'name': node_path.split('/')[-1] if node_path else store_name,
                'path': node_path,
                'type': node_type,
                'store_name': store_name,
                'attrs': {},
                'coords': {},
                'vars': {}
            }
            
            if node_type == 'group':
                # Get attributes
                try:
                    details['attrs'] = dict(node.attrs)
                except Exception as e:
                    print(f"WARNING: Failed to get attributes: {e}")
                
                # Special handling for yr.no group
                if clean_node_path == 'yr.no':
                    print(f"Special handling for yr.no group in get_node_details")
                    try:
                        # Use store.list_dir() to find weather variables
                        store_keys = list(store.list_dir(""))
                        yr_no_keys = [key for key in store_keys if key.startswith('yr.no/') and '/' in key[6:]]
                        
                        # Extract variable names
                        variable_names = set()
                        for key in yr_no_keys:
                            parts = key.split('/')
                            if len(parts) >= 2 and parts[0] == 'yr.no':
                                var_name = parts[1]
                                if var_name and not var_name.startswith('.'):
                                    variable_names.add(var_name)
                        
                        print(f"Found yr.no variables in get_node_details: {list(variable_names)}")
                        
                        # Add variables to details (LIMITED INFO)
                        for var_name in sorted(variable_names):
                            details['vars'][var_name] = {
                                'name': var_name,
                                'path': f"{clean_node_path}/{var_name}",
                                'type': 'array',
                                'shape': 'Click to load...',
                                'dtype': 'Loading...',
                                'sample_data': '(Click variable to see data)'
                            }
                            print(f"📄 Added variable to details: {var_name}")
                        
                        print(f"SUCCESS: Extracted details for {node_path}")
                        return clean_nan_values(details)
                        
                    except Exception as e:
                        print(f"WARNING: yr.no fallback in get_node_details failed: {e}")
                
                # Get coordinates and variables
                try:
                    subgroups = list(getattr(node, 'group_keys')())
                    arrays = list(getattr(node, 'array_keys')())
                    
                    # Process coordinates (usually groups with coordinate data)
                    for name in subgroups:
                        try:
                            coord_group = node[name]
                            coord_details = self._extract_coordinate_info(coord_group, name)
                            details['coords'][name] = coord_details
                        except Exception as e:
                            print(f"WARNING: Failed to process coordinate {name}: {e}")
                    
                    # Process variables (arrays)
                    for name in arrays:
                        try:
                            var_array = node[name]
                            var_details = self._extract_variable_info(var_array, name)
                            details['vars'][name] = var_details
                        except Exception as e:
                            print(f"WARNING: Failed to process variable {name}: {e}")
                            
                except Exception as e:
                    print(f"WARNING: Failed to get group contents: {e}")
                    
            elif node_type == 'array':
                # For arrays, treat as a variable
                var_details = self._extract_variable_info(node, node_path.split('/')[-1] if node_path else store_name)
                details['vars'][node_path.split('/')[-1] if node_path else store_name] = var_details
            
            print(f"SUCCESS: Extracted details for {node_path}")
            return clean_nan_values(details)
            
        except Exception as e:
            logger.error(f"Failed to get node details for {store_name}/{node_path}: {e}")
            raise
    '''
    def _get_node_details_zarr_fuse(self, store_name: str, node_path: str) -> Dict[str, Any]:
        """New implementation using zarr_fuse library"""
        # TODO: Implement zarr_fuse integration
        raise NotImplementedError("zarr_fuse node details integration not yet implemented")
    
    def get_variable_data(self, store_name: str, variable_path: str) -> Dict[str, Any]:
        """Get actual data for a specific variable (limited sample)"""
        try:
            print(f"Getting variable data: {store_name}/{variable_path}")
            
            # Ensure config is loaded
            if not self._current_config:
                raise Exception("No endpoint configuration found for S3Service")
            
            # TODO: Implement zarr_fuse path for variable data
            # For now, use legacy implementation
            print(f"Using legacy path for variable: {variable_path}")
            
            # LEGACY CODE BELOW - NO CHANGES TO EXISTING LOGIC
            # Get store configuration
            store_url = self._current_config.store_url
            store_path = store_url[5:] if store_url.startswith('s3://') else store_url
            print(f"Using store path: {store_path}")
            
            # Create storage options
            storage_options = self._get_storage_options()
            print(f"Using storage options: {storage_options}")
            
            # Use zarr_fuse approach: FsspecStore instead of get_mapper
            fs = fsspec.filesystem('s3', asynchronous=False, **storage_options)
            clean_path = store_path  # Already clean, no s3:// prefix
            store = FsspecStore(fs, path=clean_path)
            print(f"Created FsspecStore like zarr_fuse: {clean_path}")
            
            # Open the variable array using test script approach
            try:
                print(f"Opening variable array: {variable_path}")
                
                # First open the store as group
                store_group = zarr.open_group(store, mode='r')
                print(f"Opened store group, accessing: {variable_path}")
                
                # Debug: List what's actually in the store
                print(f"Store group keys: {list(store_group.keys())}")
                
                # Generic approach: Parse full variable path
                print(f"Opening variable array: {variable_path}")
                
                try:
                    # Try direct path access first
                    variable_array = zarr.open_array(store, path=variable_path, mode='r')
                    print(f"SUCCESS: Opened variable using direct path: {variable_path}")
                except Exception as e:
                    print(f"ERROR: Direct path failed: {e}")
                    try:
                        # Alternative: Use zarr.open with full path
                        variable_array = zarr.open(store)[variable_path]
                        print(f"SUCCESS: Opened variable using zarr.open()[path]: {variable_path}")
                    except Exception as e2:
                        print(f"ERROR: Both approaches failed: {e2}")
                        return {"error": f"Failed to open variable {variable_path}: {e2}"}
                else:
                    variable_array = store_group[variable_path]
                
                print(f"SUCCESS: Opened variable array: {variable_path}")
                
                # Get basic info
                shape = variable_array.shape
                dtype = str(variable_array.dtype)
                
                # Get sample data (first few values)
                if len(shape) == 1:
                    # 1D array - get first 10 values
                    sample_data = variable_array[:10].tolist()
                elif len(shape) == 2:
                    # 2D array - get first 5x5 slice
                    sample_data = variable_array[:5, :5].tolist()
                elif len(shape) == 3:
                    # 3D array - get first slice
                    sample_data = variable_array[0, :5, :5].tolist()
                else:
                    # Higher dimensions - just get first few elements flattened
                    flat_data = variable_array.flatten()
                    sample_data = flat_data[:20].tolist()
                
                # Get min/max (from small sample to avoid memory issues)
                if len(shape) <= 2:
                    min_val = float(variable_array[:100].min()) if variable_array.size > 0 else None
                    max_val = float(variable_array[:100].max()) if variable_array.size > 0 else None
                else:
                    # For higher dimensions, use a smaller sample
                    sample = variable_array.flatten()[:1000]
                    min_val = float(sample.min()) if len(sample) > 0 else None
                    max_val = float(sample.max()) if len(sample) > 0 else None
                
                result = {
                    'name': variable_path.split('/')[-1],
                    'path': variable_path,
                    'shape': list(shape),
                    'dtype': dtype,
                    'size': int(variable_array.size),
                    'sample_data': sample_data,
                    'min': min_val,
                    'max': max_val,
                    'attrs': dict(variable_array.attrs) if hasattr(variable_array, 'attrs') else {}
                }
                return clean_nan_values(result)
                
            except Exception as e:
                print(f"WARNING: Failed to open variable array: {e}")
                return {
                    'name': variable_path.split('/')[-1],
                    'path': variable_path,
                    'error': f"Failed to load variable: {str(e)}"
                }
                
        except Exception as e:
            print(f"ERROR: Error getting variable data: {e}")
            raise
    
    def _get_variable_data_zarr_fuse(self, store_name: str, variable_path: str) -> Dict[str, Any]:
        """New implementation using zarr_fuse library"""
        # TODO: Implement zarr_fuse integration
        raise NotImplementedError("zarr_fuse integration not yet implemented")
    
    def _extract_coordinate_info(self, coord_group, name: str) -> Dict[str, Any]:
        """Extract information about a coordinate"""
        try:
            info = {
                'name': name,
                'type': 'coordinate'
            }
            
            # Get attributes
            try:
                info['attrs'] = dict(coord_group.attrs)
            except Exception:
                info['attrs'] = {}
            
            # Get arrays in the coordinate group
            arrays = list(getattr(coord_group, 'array_keys')())
            for array_name in arrays:
                try:
                    array = coord_group[array_name]
                    info['values'] = {
                        'shape': array.shape,
                        'dtype': str(array.dtype),
                        'sample_data': self._get_sample_data(array),
                        'min': float(array[:].min()) if array.size > 0 else None,
                        'max': float(array[:].max()) if array.size > 0 else None,
                        'count': array.size
                    }
                except Exception as e:
                    print(f"WARNING: Failed to process coordinate array {array_name}: {e}")
            
            return info
        except Exception as e:
            print(f"WARNING: Failed to extract coordinate info for {name}: {e}")
            return {'name': name, 'type': 'coordinate', 'error': str(e)}
    
    def _extract_variable_info(self, var_array, name: str) -> Dict[str, Any]:
        """Extract information about a variable"""
        try:
            info = {
                'name': name,
                'type': 'variable',
                'shape': var_array.shape,
                'dtype': str(var_array.dtype),
                'chunks': var_array.chunks,
                'size': var_array.size,
                'sample_data': self._get_sample_data(var_array),
                'min': float(var_array[:].min()) if var_array.size > 0 else None,
                'max': float(var_array[:].max()) if var_array.size > 0 else None
            }
            
            # Get attributes
            try:
                info['attrs'] = dict(var_array.attrs)
            except Exception:
                info['attrs'] = {}
            
            return info
        except Exception as e:
            print(f"WARNING: Failed to extract variable info for {name}: {e}")
            return {'name': name, 'type': 'variable', 'error': str(e)}

# Global S3 service instance
s3_service = S3Service()
