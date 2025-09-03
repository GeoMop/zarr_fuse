import s3fs
import zarr
import logging
from typing import Dict, Any, Optional, List
from core.config_manager import config_manager, EndpointConfig
import fsspec

logger = logging.getLogger(__name__)

def get_s3_storage_options(access_key: str, secret_key: str, endpoint_url: str) -> Dict[str, Any]:
    """Get common S3 storage options to avoid code duplication"""
    return {
        'key': access_key,
        'secret': secret_key,
        'endpoint_url': endpoint_url,
        'listings_expiry_time': 1,
        'max_paths': 0,
        'asynchronous': False,
        'config_kwargs': {
            'request_checksum_calculation': 'when_required',
            'response_checksum_validation': 'when_required',
        }
    }

class S3Service:
    """Service for S3 operations with custom S3 configuration"""
    
    def __init__(self):
        self._fs: Optional[s3fs.S3FileSystem] = None
        self._current_config: Optional[EndpointConfig] = None
    
    def connect(self, endpoint_config: EndpointConfig) -> bool:
        """Connect to S3 using common S3 configuration"""
        try:
            # Use fsspec.get_mapper approach for listing
            storage_options = dict(
                key=endpoint_config.S3_access_key,
                secret=endpoint_config.S3_secret_key,
                client_kwargs=dict(endpoint_url=endpoint_config.S3_ENDPOINT_URL),
                config_kwargs={
                    "s3": {
                        "payload_signing_enabled": False,
                        "addressing_style": "path"
                    },
                    "retries": {"max_attempts": 5, "mode": "standard"},
                    "connect_timeout": 20,
                    "read_timeout": 60,
                    "request_checksum_calculation": "when_required",
                    "response_checksum_validation": "when_required",
                },
            )
            
            # Use fsspec.filesystem for listing operations
            self._fs = fsspec.filesystem('s3', **storage_options)
            self._current_config = endpoint_config
            logger.info(f"Successfully connected to S3: {endpoint_config.STORE_URL}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to S3: {e}")
            print(f"Connection error details: {str(e)}")
            return False
    
    def get_store_structure(self) -> Dict[str, Any]:
        """Get the structure of all Zarr stores in the bucket"""
        if not self._fs:
            raise ValueError("Not connected to S3")
        
        try:
            # Get bucket name from STORE_URL
            bucket_name = self._current_config.STORE_URL.split('/')[2]
            print(f"🔍 Searching bucket: {bucket_name}")
            
            # List all items in the bucket
            bucket_items = self._fs.ls(bucket_name, detail=True)
            print(f"📁 Found {len(bucket_items)} items in bucket")
            
            # Debug: print all items
            for item in bucket_items:
                print(f"  {'📁' if item['type'] == 'directory' else '📄'} {item['name']} (ends with .zarr: {item['name'].endswith('.zarr')})")
            
            # Find all Zarr stores (directories ending with .zarr)
            zarr_stores = []
            
            # First, check root level items
            for item in bucket_items:
                if item['type'] == 'directory' and item['name'].endswith('.zarr'):
                    print(f"🎯 Found Zarr store: {item['name']}")
                    store_name = item['name'].split('/')[-1]  # Get just the store name
                    store_path = item['name']
                    
                    # Try to open the Zarr store
                    try:
                        store_structure = self._extract_structure(store_path)
                        zarr_stores.append({
                            'name': store_name,
                            'path': store_path,
                            'type': 'zarr_store',
                            'structure': store_structure
                        })
                        print(f"✅ Successfully processed: {store_name}")
                    except Exception as e:
                        # If we can't read the store, still list it but mark as error
                        zarr_stores.append({
                            'name': store_name,
                            'path': store_path,
                            'type': 'zarr_store',
                            'error': f"Could not read store: {str(e)}"
                        })
                        print(f"❌ Error processing: {store_name} - {e}")
            
            # Then, check subdirectories for .zarr files
            for item in bucket_items:
                if item['type'] == 'directory' and not item['name'].endswith('.zarr'):
                    print(f"🔍 Checking subdirectory: {item['name']}")
                    try:
                        sub_items = self._fs.ls(item['name'], detail=True)
                        for sub_item in sub_items:
                            print(f"    {'📁' if sub_item['type'] == 'directory' else '📄'} {sub_item['name']} (ends with .zarr: {sub_item['name'].endswith('.zarr')})")
                            
                            if sub_item['type'] == 'directory' and sub_item['name'].endswith('.zarr'):
                                print(f"🎯 Found Zarr store in subdirectory: {sub_item['name']}")
                                store_name = sub_item['name'].split('/')[-1]  # Get just the store name
                                store_path = sub_item['name']
                                
                                # Try to open the Zarr store
                                try:
                                    store_structure = self._extract_structure(store_path)
                                    zarr_stores.append({
                                        'name': store_name,
                                        'path': store_path,
                                        'type': 'zarr_store',
                                        'structure': store_structure
                                    })
                                    print(f"✅ Successfully processed: {store_name}")
                                except Exception as e:
                                    # If we can't read the store, still list it but mark as error
                                    zarr_stores.append({
                                        'name': store_name,
                                        'path': store_path,
                                        'type': 'zarr_store',
                                        'error': f"Could not read store: {str(e)}"
                                    })
                                    print(f"❌ Error processing: {store_name} - {e}")
                    except Exception as e:
                        print(f"❌ Error listing subdirectory {item['name']}: {e}")
            
            print(f"📊 Total Zarr stores found: {len(zarr_stores)}")
            
            # Debug: print the structure of the first store
            if zarr_stores and zarr_stores[0].get('structure'):
                print("🔍 First store structure:")
                import json
                print(json.dumps(zarr_stores[0]['structure'], indent=2, default=str))
            
            return {
                'status': 'success',
                'bucket_name': bucket_name,
                'total_stores': len(zarr_stores),
                'stores': zarr_stores
            }
            
        except Exception as e:
            logger.error(f"Failed to get store structure: {e}")
            raise ValueError(f"Failed to get store structure: {e}")
    
    def _extract_structure(self, store_path: str) -> Dict[str, Any]:
        """Extract structure from a Zarr store"""
        try:
            print(f"🔍 Extracting structure from: {store_path}")
            
            # Use fsspec.get_mapper like test script
            storage_options = dict(
                key=self._current_config.S3_access_key,
                secret=self._current_config.S3_secret_key,
                client_kwargs=dict(endpoint_url=self._current_config.S3_ENDPOINT_URL),
                config_kwargs={
                    "s3": {
                        "payload_signing_enabled": False,
                        "addressing_style": "path"
                    },
                    "retries": {"max_attempts": 5, "mode": "standard"},
                    "connect_timeout": 20,
                    "read_timeout": 60,
                    "request_checksum_calculation": "when_required",
                    "response_checksum_validation": "when_required",
                },
            )
            
            print(f"🔧 Using storage options: {storage_options}")
            print(f"🔍 Original store_path: {store_path}")
            bucket_name = self._current_config.STORE_URL.split('/')[2]
            # Normalize path and avoid duplicating bucket
            normalized_path = store_path.lstrip('/')
            if normalized_path.startswith(f"{bucket_name}/"):
                mapper_url = f"s3://{normalized_path}"
            else:
                mapper_url = f"s3://{bucket_name}/{normalized_path}"
            print(f"🔗 Mapper URL: {mapper_url}")
            store = fsspec.get_mapper(mapper_url, **storage_options)
            print(f"✅ Created mapper for: {mapper_url}")
            
            # Open the Zarr store - try different approaches
            store_group = None
            print(f"🔍 Attempting to open store: {store_path}")
            
            try:
                # First try to open as a group
                print(f"🔍 Trying to open as group...")
                store_group = zarr.open_group(store, mode='r')
                print(f"✅ Opened Zarr store as group: {store_path}")
            except Exception as e:
                print(f"⚠️  Failed to open as group: {e}")
                try:
                    # Try to open as an array (some stores might be just arrays)
                    print(f"🔍 Trying to open as array...")
                    store_group = zarr.open_array(store, mode='r')
                    print(f"✅ Opened Zarr store as array: {store_path}")
                    # Convert array to group-like structure
                    return {
                        'name': store_path.split('/')[-1],
                        'path': store_path,
                        'type': 'array',
                        'shape': store_group.shape,
                        'dtype': str(store_group.dtype),
                        'chunks': store_group.chunks,
                        'size': store_group.size,
                        'sample_data': self._get_sample_data(store_group)
                    }
                except Exception as e2:
                    print(f"⚠️  Failed to open as array: {e2}")
                    # Try to list contents directly
                    try:
                        print(f"🔍 Trying to list store contents directly...")
                        contents = list(store.keys())
                        print(f"📁 Store contents: {contents}")
                        if contents:
                            # Create a minimal group structure
                            return {
                                'name': store_path.split('/')[-1],
                                'path': store_path,
                                'type': 'group',
                                'children': [{'name': item, 'path': item, 'type': 'unknown'} for item in contents]
                            }
                    except Exception as e3:
                        print(f"⚠️  Failed to list contents: {e3}")
                        raise e  # Re-raise original error
            
            if store_group is None:
                raise Exception("Could not open store in any format")
                
            try:
                g_keys = list(getattr(store_group, 'group_keys')())
            except Exception:
                g_keys = []
            try:
                a_keys = list(getattr(store_group, 'array_keys')())
            except Exception:
                a_keys = []
            print(f"📁 Store group_keys: {g_keys}")
            print(f"📄 Store array_keys: {a_keys}")
            
            def _process_group(group, group_path):
                """Recursively process a Zarr group"""
                result = {
                    'name': group_path.split('/')[-1] if group_path else 'root',
                    'path': group_path,
                    'type': 'group',
                    'children': []
                }
                
                try:
                    subgroups = list(getattr(group, 'group_keys')())
                except Exception:
                    subgroups = []
                try:
                    arrays = list(getattr(group, 'array_keys')())
                except Exception:
                    arrays = []
                print(f"📁 Processing group: {result['name']} with subgroups={subgroups}, arrays={arrays}")
                
                # Process arrays first
                for name in arrays:
                    try:
                        dataset = group[name]
                        print(f"📄 Found array: {name} shape={getattr(dataset, 'shape', None)}")
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
                        print(f"⚠️  Failed to process array {name}: {e}")

                # Then process subgroups
                for name in subgroups:
                    try:
                        subgroup = group[name]
                        print(f"📁 Found subgroup: {name}")
                        subgroup_result = _process_group(subgroup, f"{group_path}/{name}" if group_path else name)
                        result['children'].append(subgroup_result)
                    except Exception as e:
                        print(f"⚠️  Failed to process subgroup {name}: {e}")
                
                print(f"✅ Finished processing group: {result['name']} with {len(result['children'])} children")
                return result
            
            final_result = _process_group(store_group, "")
            print(f"🎯 Final structure: {final_result['name']} with {len(final_result['children'])} children")
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
                return sample_data.tolist()
            else:
                return []
        except Exception as e:
            logger.warning(f"Failed to get sample data for dataset: {e}")
            return []
    
    def get_array_data(self, array_path: str, slice_info: Optional[Dict] = None) -> Dict[str, Any]:
        """Get data from a specific array"""
        if not self._fs or not self._current_config:
            raise ValueError("Not connected to S3. Call connect() first.")
        
        try:
            store_url = self._current_config.STORE_URL
            if store_url.startswith('s3://'):
                store_path = store_url[5:]
            else:
                store_path = store_url
            
            full_path = f"{store_path}/{array_path}"
            
            if not self._fs.exists(full_path):
                raise FileNotFoundError(f"Array not found: {array_path}")
            
            # Open the array
            array = zarr.open_array(store=self._fs.get_mapper(store_path), path=array_path)
            
            # Get basic info
            data_info = {
                'path': array_path,
                'shape': array.shape,
                'dtype': str(array.dtype),
                'chunks': array.chunks,
                'size': array.size
            }
            
            # Get actual data if slice_info is provided
            if slice_info:
                # For now, just get a small sample
                sample_slice = tuple(slice(0, min(10, dim)) for dim in array.shape)
                data_info['sample_data'] = array[sample_slice].tolist()
            
            return data_info
            
        except Exception as e:
            logger.error(f"Failed to get array data for {array_path}: {e}")
            raise

    def get_node_details(self, store_name: str, node_path: str) -> Dict[str, Any]:
        """Get detailed information about a specific node in a Zarr store"""
        if not self._fs or not self._current_config:
            raise ValueError("Not connected to S3. Call connect() first.")
        
        try:
            print(f"🔍 Getting details for node: {store_name}/{node_path}")
            
            # Find the store path
            store_path = None
            bucket_name = self._current_config.STORE_URL.split('/')[2]
            
            # Check if it's a direct store
            try:
                bucket_items = self._fs.ls(bucket_name, detail=True)
                for item in bucket_items:
                    if item['name'] == f"{bucket_name}/{store_name}":
                        store_path = item['name']
                        break
                
                if not store_path:
                    # Check subdirectories
                    for item in bucket_items:
                        if item['type'] == 'directory' and not item['name'].endswith('.zarr'):
                            try:
                                sub_items = self._fs.ls(item['name'], detail=True)
                                for sub_item in sub_items:
                                    if sub_item['name'].endswith(f"/{store_name}"):
                                        store_path = sub_item['name']
                                        break
                                if store_path:
                                    break
                            except Exception as e:
                                print(f"⚠️  Failed to list subdirectory {item['name']}: {e}")
                                continue
            except Exception as e:
                print(f"⚠️  Failed to list bucket contents: {e}")
                raise FileNotFoundError(f"Store not found: {store_name}")
            
            if not store_path:
                raise FileNotFoundError(f"Store not found: {store_name}")
            
            print(f"📁 Found store at: {store_path}")
            
            # Open the store
            storage_options = dict(
                key=self._current_config.S3_access_key,
                secret=self._current_config.S3_secret_key,
                client_kwargs=dict(endpoint_url=self._current_config.S3_ENDPOINT_URL),
                config_kwargs={
                    "s3": {
                        "payload_signing_enabled": False,
                        "addressing_style": "path"
                    },
                    "retries": {"max_attempts": 5, "mode": "standard"},
                    "connect_timeout": 20,
                    "read_timeout": 60,
                    "request_checksum_calculation": "when_required",
                    "response_checksum_validation": "when_required",
                },
            )
            
            store = fsspec.get_mapper(f"s3://{store_path}", **storage_options)
            
            # Try to open as group first
            try:
                # Remove 'root/' prefix if present
                clean_node_path = node_path.replace('root/', '') if node_path.startswith('root/') else node_path
                
                # Special handling for root node
                if clean_node_path == 'root' or clean_node_path == '':
                    node = zarr.open_group(store, mode='r')
                    node_type = 'group'
                    print(f"✅ Opened root group")
                elif clean_node_path:
                    node = zarr.open_group(store, path=clean_node_path, mode='r')
                    node_type = 'group'
                    print(f"✅ Opened as group: {clean_node_path}")
                else:
                    node = zarr.open_group(store, mode='r')
                    node_type = 'group'
                    print(f"✅ Opened as group: {clean_node_path}")
            except Exception as e:
                print(f"⚠️  Failed to open as group: {e}")
                try:
                    # Remove 'root/' prefix if present
                    clean_node_path = node_path.replace('root/', '') if node_path.startswith('root/') else node_path
                    
                    # Special handling for root node
                    if clean_node_path == 'root' or clean_node_path == '':
                        node = zarr.open_array(store, mode='r')
                        node_type = 'array'
                        print(f"✅ Opened root array")
                    elif clean_node_path:
                        node = zarr.open_array(store, path=clean_node_path, mode='r')
                        node_type = 'array'
                        print(f"✅ Opened as array: {clean_node_path}")
                    else:
                        node = zarr.open_array(store, mode='r')
                        node_type = 'array'
                        print(f"✅ Opened as array: {clean_node_path}")
                except Exception as e2:
                    print(f"⚠️  Failed to open as array: {e2}")
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
                    print(f"⚠️  Failed to get attributes: {e}")
                
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
                            print(f"⚠️  Failed to process coordinate {name}: {e}")
                    
                    # Process variables (arrays)
                    for name in arrays:
                        try:
                            var_array = node[name]
                            var_details = self._extract_variable_info(var_array, name)
                            details['vars'][name] = var_details
                        except Exception as e:
                            print(f"⚠️  Failed to process variable {name}: {e}")
                            
                except Exception as e:
                    print(f"⚠️  Failed to get group contents: {e}")
                    
            elif node_type == 'array':
                # For arrays, treat as a variable
                var_details = self._extract_variable_info(node, node_path.split('/')[-1] if node_path else store_name)
                details['vars'][node_path.split('/')[-1] if node_path else store_name] = var_details
            
            print(f"✅ Extracted details for {node_path}")
            return details
            
        except Exception as e:
            logger.error(f"Failed to get node details for {store_name}/{node_path}: {e}")
            raise
    
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
                    print(f"⚠️  Failed to process coordinate array {array_name}: {e}")
            
            return info
        except Exception as e:
            print(f"⚠️  Failed to extract coordinate info for {name}: {e}")
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
            print(f"⚠️  Failed to extract variable info for {name}: {e}")
            return {'name': name, 'type': 'variable', 'error': str(e)}

# Global S3 service instance
s3_service = S3Service()
