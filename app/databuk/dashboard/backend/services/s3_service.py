import zarr
from zarr.storage import FsspecStore
import logging
import math
import os
from pathlib import Path
from typing import Dict, Any, Optional, List
from core.config_manager import load_endpoints, get_first_endpoint, EndpointConfig
import fsspec
import xarray as xr
import numpy as np

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


def find_dataframe_column(df, primary_names: List[str], exclude_keywords: Optional[List[str]] = None) -> Optional[str]:
    """
    Find a DataFrame column by name with fuzzy matching and exclusion filters.
    
    Attempts to match column names in order of priority:
    1. Exact match (case-insensitive)
    2. Fuzzy search excluding specified keywords
    
    Args:
        df: Pandas DataFrame to search
        primary_names: List of preferred column names to match (in priority order)
        exclude_keywords: Keywords to exclude in fuzzy search (e.g., ['lon'] when searching for latitude)
        
    Returns:
        Column name if found, None otherwise
    """
    all_cols = df.columns.tolist()
    lower_cols = [c.lower() for c in all_cols]
    
    # Step 1: Try exact match
    for name in primary_names:
        if name.lower() in lower_cols:
            return all_cols[lower_cols.index(name.lower())]
    
    # Step 2: Fuzzy search with exclusion
    for name in primary_names:
        exclude_set = set(exclude_keywords or [])
        candidates = [
            all_cols[i] for i, col in enumerate(lower_cols)
            if name.lower() in col and not any(ex.lower() in col for ex in exclude_set)
        ]
        if candidates:
            return candidates[0]
    
    return None


class S3Service:
    """Service for S3 operations with custom S3 configuration"""

    def get_plot_json(self, store_name, node_path, plot_type=None, selection=None):
        """Return plot-ready JSON for dashboard visualizations."""
        try:
            logger.debug(f"Generating plot for {node_path}, type={plot_type}")
            
            # 1. Open Store & Node
            _, root_node = self._open_zarr_store()
            
            # Navigate to node
            parts = [p for p in node_path.split('/') if p]
            current_node = root_node
            
            # Debug info
            logger.debug(f"Root node name: {getattr(root_node, 'name', 'unknown')}")
            if hasattr(root_node, 'children'):
                logger.debug(f"Root children: {list(root_node.children.keys())}")
            
            for part in parts:
                # If the part matches the current node's name, it might be redundant path info
                # But usually we want to traverse down.
                
                if hasattr(current_node, 'children') and part in current_node.children:
                    current_node = current_node.children[part]
                elif part == getattr(current_node, 'name', ''):
                    # If the path part is the same as current node name, skip it (it's redundant)
                    continue
                else:
                    # If not found in children, check if it's the node itself (root case)
                    # This is a bit tricky. If we are at root and path is "yr.no", and root has child "yr.no", we go there.
                    # If root IS "yr.no", then we stay.
                    if part == getattr(current_node, 'name', ''):
                         continue
                         
                    logger.error(f"Failed to find {part} in {getattr(current_node, 'name', 'unknown')}")
                    if hasattr(current_node, 'children'):
                        logger.debug(f"Available children: {list(current_node.children.keys())}")
                    
                    raise ValueError(f"Node {part} not found in path {node_path}")
            
            # 2. Prepare Data
            if not hasattr(current_node, 'dataset') or not current_node.dataset:
                 raise ValueError("Node does not contain a dataset")

            ds = current_node.dataset
            var_names = list(ds.data_vars.keys())
            
            if not var_names:
                raise ValueError("No variables found in dataset")

            # Load DataFrame
            logger.debug(f"Loading data for variables: {var_names}")
            
            # Helper to get all relevant variables including composed coords
            all_vars = list(var_names)
            try:
                if hasattr(ds, 'coords'):
                    dims = set()
                    for coord_name in ds.coords:
                        if hasattr(ds.coords[coord_name], 'attrs') and 'composed' in ds.coords[coord_name].attrs:
                            dims = dims.union(set(ds.coords[coord_name].attrs['composed']))
                    all_vars.extend(list(dims))
                all_vars = list(set([v for v in all_vars if v in ds]))
            except Exception as e:
                logger.warning(f"Error determining composed variables: {e}")
            
            df = None
            
            # --- TIMESERIES LOGIC ---
            if plot_type == 'timeseries':
                logger.debug(f"Processing timeseries for selection: {selection}")
                if not selection or 'lat_point' not in selection or 'lon_point' not in selection:
                     return {"status": "error", "reason": "Missing lat_point/lon_point for timeseries"}
                
                try:
                    req_lat = float(selection['lat_point'])
                    req_lon = float(selection['lon_point'])
                    
                    # Helper to find var in ds (case insensitive)
                    def get_var(names):
                        for n in names:
                            for v in ds.variables:
                                if v.lower() == n: return ds[v]
                        return None

                    lat_arr = get_var(['latitude', 'lat'])
                    lon_arr = get_var(['longitude', 'lon'])
                    
                    if lat_arr is not None and lon_arr is not None:
                        # Load arrays to memory
                        lats = lat_arr.values
                        lons = lon_arr.values
                        
                        # Simple Euclidean distance
                        dist = (lats - req_lat)**2 + (lons - req_lon)**2
                        min_idx = dist.argmin()
                        
                        logger.debug(f"Selected nearest point index: {min_idx} for coords ({req_lat}, {req_lon})")
                        
                        # Handle 1D vs 2D coordinates
                        ds_point = None
                        if lat_arr.ndim == 1:
                            if lat_arr.dims:
                                dim_name = lat_arr.dims[0]
                                ds_point = ds.isel({dim_name: min_idx})
                            else:
                                return {"status": "error", "reason": "Latitude variable has no dimensions"}
                        elif lat_arr.ndim == 2:
                            # Unravel index for 2D grid
                            unraveled_idx = np.unravel_index(min_idx, lat_arr.shape)
                            logger.debug(f"Unraveled 2D index: {unraveled_idx}")
                            
                            # Map dimensions to indices
                            if len(lat_arr.dims) == 2:
                                selector = {
                                    lat_arr.dims[0]: unraveled_idx[0],
                                    lat_arr.dims[1]: unraveled_idx[1]
                                }
                                ds_point = ds.isel(selector)
                            else:
                                return {"status": "error", "reason": f"Latitude has 2 dimensions but dims attribute is {lat_arr.dims}"}
                        else:
                             return {"status": "error", "reason": f"Unsupported coordinate dimensionality: {lat_arr.ndim}"}

                        logger.debug("Sliced dataset for timeseries")
                        
                        df = ds_point.to_dataframe().reset_index()
                        
                        # Return raw data for frontend to handle
                        result = {
                            "status": "success",
                            "plot_type": "timeseries",
                            "data": df.to_dict(orient='list'),
                            "meta": {
                                "selected_lat": float(lats.flat[min_idx]),
                                "selected_lon": float(lons.flat[min_idx])
                            }
                        }
                        return clean_nan_values(result)
                    else:
                        return {"status": "error", "reason": "Could not find latitude/longitude variables"}
                except Exception as e:
                    logger.exception(f"Error in timeseries processing: {e}")
                    return {"status": "error", "reason": str(e)}

            # Optimization: Read only the first time step for the map
            if plot_type == 'map' and 'date_time' in ds.coords and ds.coords['date_time'].size > 1:
                logger.debug("Optimization: Reading only first time step using isel")
                try:
                    # Use isel directly on xarray dataset to avoid DatetimeIndex slicing issues
                    ds_subset = ds[all_vars].isel(date_time=slice(0, 1))
                    logger.debug("Converting sliced dataset to Pandas DataFrame")
                    df = ds_subset.to_dataframe().reset_index()
                except Exception as e:
                    logger.warning(f"Optimization with isel failed: {e}")
            
            # Fallback if optimization failed or wasn't applied
            if df is None:
                if hasattr(current_node, 'read_df'):
                    logger.debug("Using read_df for full load")
                    df = current_node.read_df(var_names)
                    if hasattr(df, 'to_pandas'):
                        df = df.to_pandas()
                else:
                    logger.debug("Using xarray to_dataframe for full load")
                    df = ds.to_dataframe().reset_index()

            # 3. Generate Plot
            if plot_type == 'map':
                # Extract time from selection if available
                time_point = selection.get('time_point') if selection else None
                
                # Identify columns using utility function
                lat_col = find_dataframe_column(df, ['latitude', 'lat', 'y'], exclude_keywords=['lon'])
                lon_col = find_dataframe_column(df, ['longitude', 'lon', 'x'], exclude_keywords=['lat'])
                time_col = find_dataframe_column(df, ['date_time', 'time', 'datetime', 'date'])
                
                # Use defaults if not found
                if not lat_col:
                    lat_col = 'latitude'
                if not lon_col:
                    lon_col = 'longitude'
                if not time_col:
                    time_col = 'date_time'

                logger.debug(f"Selected columns for plot: lat='{lat_col}', lon='{lon_col}', time='{time_col}'")
                logger.debug(f"DataFrame head columns: {df.columns.tolist()}")
                logger.debug(f"DataFrame head:\n{df[[lat_col, lon_col, time_col]].head()}")

                # Extract borehole ID if available
                borehole_id = None
                if 'borehole' in df.columns:
                    borehole_id = df['borehole'].iloc[0]
                
                from services.plot_service import generate_map_figure
                figure = generate_map_figure(df, time_point, lat_col, lon_col, time_col)
                
                # Add borehole information to meta
                if isinstance(figure, dict):
                    if 'meta' not in figure:
                        figure['meta'] = {}
                    if borehole_id:
                        figure['meta']['borehole_id'] = str(borehole_id)
                
                return figure
            
            else:
                return {"status": "error", "reason": f"Unknown plot type: {plot_type}"}

        except Exception as e:
            logger.exception(f"Failed to generate plot: {e}")
            return {"status": "error", "reason": str(e)}

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
        # import yaml
        # with open(schema_path, 'r', encoding='utf-8') as f:
        #     raw_dict = yaml.safe_load(f)
        #     from zarr_fuse.zarr_schema import SchemaAddress
        #     address = SchemaAddress(addr=[], file=self._current_config.store_url)
        #     schema = zarr_fuse.zarr_schema.dict_deserialize(raw_dict, address)
        #kwargs = {"S3_ENDPOINT_URL": os.getenv("ZF_S3_ENDPOINT_URL")}
        node = zarr_fuse.open_store(Path(schema_path))
        return None, node
    
    def _find_group_variables(self, store, store_group, group_path: str = "") -> List[str]:
        """Find available variables in a group using multiple fallback approaches"""
        
        # Method 1: Use store.list_dir() (preferred for FsspecStore)
        try:
            store_keys = list(store.list_dir(""))
            logger.debug(f"Store list_dir result: {len(store_keys)} items")
            
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
                logger.info(f"Found {len(group_variables)} variables using list_dir in {group_path or 'root'}")
                return group_variables
        except Exception as e:
            logger.warning(f"list_dir method failed: {e}")
        
        # Method 2: Access group directly
        try:
            if group_path:
                target_group = store_group[group_path]
            else:
                target_group = store_group
            group_variables = list(target_group.keys())
            logger.info(f"Found {len(group_variables)} variables using group keys in {group_path or 'root'}")
            return group_variables
        except Exception as e:
            logger.warning(f"Group access method failed: {e}")
        
        # Both methods failed
        logger.error(f"Could not find variables in {group_path or 'root'} using any method")
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
                            logger.warning(f"Could not process array {name}: {e}")
                    
                    return result
                    
                except Exception as e:
                    logger.warning(f"Store-level variable detection failed for {group_path_attr}: {e}")
        
        for key in all_subkeys:
            try:
                item = group[key]
                if hasattr(item, 'shape'):  # It's an array
                    arrays.append(key)
                else:  # It's a group
                    subgroups.append(key)
            except Exception as e:
                logger.warning(f"Could not access subkey {key}: {e}")
        
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
            def node_to_legacy_minimal(node, path=""):
                children = []
                # Variables (arrays) - sadece isim ve path
                if hasattr(node, 'dataset') and node.dataset:
                    ds = node.dataset
                    if hasattr(ds, 'data_vars'):
                        for var_name in ds.data_vars.keys():
                            children.append({
                                'name': var_name,
                                'path': f"{path}/{var_name}" if path else var_name,
                                'type': 'array'
                            })
                # Subgroups (children)
                if hasattr(node, 'children') and node.children:
                    for child_name, child_node in node.children.items():
                        children.append(node_to_legacy_minimal(child_node, f"{path}/{child_name}" if path else child_name))
                return {
                    'name': node.name if hasattr(node, 'name') else 'root',
                    'path': path,
                    'type': 'group',
                    'children': children
                }

            legacy_structure = node_to_legacy_minimal(node)
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
        """Get detailed information about a specific node in a Zarr store (modern sidebar logic, dynamic children)"""
        try:
            if not self._fs or not self._current_config:
                raise ValueError("Not connected to S3. Call connect() first.")
            # Open Zarr store
            _, node = self._open_zarr_store()
            # Split path into parts
            parts = [p for p in node_path.split('/') if p]
            print("Requested node_path:", node_path)
            current = node
            for idx, part in enumerate(parts):
                if hasattr(current, 'children'):
                    print(f"Step {idx}: Looking for part '{part}' in children: {list(current.children.keys())}")
                    if part in current.children:
                        current = current.children[part]
                    else:
                        print(f"Part '{part}' not found at step {idx}!")
                        return None
                else:
                    print(f"No children attribute at step {idx} for part '{part}'!")
                    return None
            # Build children dynamically: groups and arrays
            children = []
            # Add subgroups (recursive)
            if hasattr(current, 'children') and current.children:
                for child_name, child_node in current.children.items():
                    children.append({
                        'name': child_name,
                        'type': 'group' if hasattr(child_node, 'children') and child_node.children else 'array',
                        'path': f"{node_path}/{child_name}" if node_path else child_name
                    })
            # Add arrays from dataset
            if hasattr(current, 'dataset') and current.dataset:
                ds = current.dataset
                if hasattr(ds, 'data_vars'):
                    for var_name in ds.data_vars.keys():
                        children.append({
                            'name': var_name,
                            'type': 'array',
                            'path': f"{node_path}/{var_name}" if node_path else var_name
                        })
            # If array, return details
            if hasattr(current, 'dataset') and current.dataset:
                ds = current.dataset
                # Fill vars and coords from xarray dataset if available
                vars_list = []
                coords_list = []
                # Variables (data_vars)
                if hasattr(ds, 'data_vars'):
                    for var_name in ds.data_vars.keys():
                        var = ds.data_vars[var_name]
                        try:
                            if len(var.shape) == 1:
                                sample_data = var[:10].tolist()
                            elif len(var.shape) == 2:
                                sample_data = var[:5, :5].tolist()
                            else:
                                sample_data = []
                        except Exception:
                            sample_data = []
                        vars_list.append({
                            'name': var_name,
                            'path': f"{node_path}/{var_name}" if node_path else var_name,
                            'shape': list(var.shape),
                            'dtype': str(var.dtype),
                            'size': getattr(var, 'size', None),
                            'attrs': dict(var.attrs) if hasattr(var, 'attrs') else {},
                            'sample_data': sample_data
                        })
                # Coordinates (coords)
                if hasattr(ds, 'coords'):
                    for coord_name in ds.coords.keys():
                        coord = ds.coords[coord_name]
                        try:
                            if len(coord.shape) == 1:
                                sample_data = coord[:10].tolist()
                            elif len(coord.shape) == 2:
                                sample_data = coord[:5, :5].tolist()
                            else:
                                sample_data = []
                        except Exception:
                            sample_data = []
                        coords_list.append({
                            'name': coord_name,
                            'shape': list(coord.shape),
                            'dtype': str(coord.dtype),
                            'size': getattr(coord, 'size', None),
                            'attrs': dict(coord.attrs) if hasattr(coord, 'attrs') else {},
                            'sample_data': sample_data
                        })
                # If single variable, return array details
                if hasattr(ds, 'data_vars') and len(ds.data_vars) == 1:
                    var_obj = vars_list[0]
                    return {
                        'name': var_obj['name'],
                        'path': node_path,
                        'type': 'array',
                        'shape': var_obj['shape'],
                        'dtype': var_obj['dtype'],
                        'size': var_obj['size'],
                        'attrs': var_obj['attrs'],
                        'vars': vars_list,
                        'coords': coords_list,
                        'sample_data': var_obj['sample_data']
                    }
                # If group, return group details with vars/coords
                return {
                    'name': getattr(current, 'name', node_path.split('/')[-1]),
                    'path': node_path,
                    'type': 'group',
                    'children': children,
                    'attrs': {},
                    'vars': vars_list,
                    'coords': coords_list
                }
            # Return group node details (no dataset)
            return {
                'name': getattr(current, 'name', node_path.split('/')[-1]),
                'path': node_path,
                'type': 'group',
                'children': children,
                'attrs': {},
                'vars': [],
                'coords': []
            }
        except Exception as e:
            logger.error(f"Failed to get node details for {store_name}/{node_path}: {e}")
            raise
    
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
                    # New implementation using zarr_fuse library
                    # TODO: Implement zarr_fuse integration
        raise NotImplementedError("zarr_fuse node details integration not yet implemented")
s3_service = S3Service()
