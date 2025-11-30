import inspect
import warnings
import logging

import json

# Route warnings through logging
logging.captureWarnings(True)


import re
import os
from typing import List, Callable, Dict, Optional, Set, Tuple, Union, Any
import polars as pl
import xarray as xr
import numpy as np
import zarr
from zarr.errors import ZarrUserWarning, GroupNotFoundError

import fsspec
import asyncio
from pathlib import Path
from functools import cached_property

from . import zarr_schema, units
from .schema_ctx import RaisingLogger
from .dtype_converter import to_typed_array, TrimmedArrayWarning
from .logger import get_logger
from .interpolate import interpolate_ds
from .zarr_schema import DatasetSchema, NodeSchema
from .tools import recursive_update
"""
tuple val coords:
- source cols as quantities
- Coordinates object holds both index and non-dim coordinates
  so we use them. We just form the index coodrinate by hash.
- How to allow providint tuple coord values in structure. 
  Ideal as dictionary for source cols, but then there is duplicity of their names
  Rather have values as dict determining source columns, optionaly providing values 

- pivot_nd - hash source cols
- future read - hash source cols

TODO:
- strict separation of read only and append/write operations

- schema and dataset should be read only
  If the group does not exist, should return None , empty or raise error.
- Write operations: logger (see also LoggingStore), and update operations.
"""


def call_with_filtered_kwargs(func, *args, **kwargs):
    """
    Call a function with the provided arguments, filtering out any keyword arguments
    that the function does not accept.

    If the function accepts arbitrary keyword arguments (i.e. has a **kwargs parameter),
    then no filtering is performed.

    Parameters:
        func (callable): The function to call.
        *args: Positional arguments for the function.
        **kwargs: Keyword arguments for the function.

    Returns:
        The result of calling func with the filtered arguments.
    """
    # Retrieve the signature of the function.
    sig = inspect.signature(func)
    parameters = sig.parameters

    # Check if the function accepts arbitrary keyword arguments.
    if any(p.kind == p.VAR_KEYWORD for p in parameters.values()):
        return func(*args, **kwargs)

    # Filter out kwargs not accepted by the function.
    valid_kwargs = {}
    for name, param in parameters.items():
        # For positional/keyword and keyword-only parameters,
        # include the kwarg if it's provided.
        if param.kind in (param.POSITIONAL_OR_KEYWORD, param.KEYWORD_ONLY):
            if name in kwargs:
                valid_kwargs[name] = kwargs[name]

    return func(*args, **valid_kwargs)


class ZFOptionError(Exception):
    pass

def _get_option(store_options, key, **kwargs):
    try:
        return store_options[key]
    except KeyError:
        pass

    try:
        return kwargs['default']
    except KeyError:
        pass

    raise ZFOptionError(f"Missing mandatory option '{key}'.")





def _s3_options(store_options):
    store_url = _get_option(store_options, 'STORE_URL') # The only mandatory parameter
    # Check for S3 URLs first
    # Create S3 filesystem with environment variables
    s3_options = {
        'key': _get_option(store_options, 'S3_ACCESS_KEY'),
        'secret': _get_option(store_options, 'S3_SECRET_KEY'),
        'endpoint_url': _get_option(store_options, 'S3_ENDPOINT_URL'),
        'listings_expiry_time': 1,
        'max_paths': 0,
        'asynchronous': False,
        'config_kwargs': {
            #'s3': {
            #    'payload_signing_enabled': False,
            #    'addressing_style': os.getenv('S3_ADDRESSING_STYLE'),
            #},
            #'retries': {'max_attempts': 5, 'mode': 'standard'},
            #'connect_timeout': 20,
            #'read_timeout': 60,
            'request_checksum_calculation': 'when_required',
            'response_checksum_validation': 'when_required',
        }
    }
    custom_options_json = _get_option(store_options, 'S3_OPTIONS', default='{}')
    custom_options = json.loads(custom_options_json)
    return recursive_update(s3_options, custom_options)

def _zarr_store_open(store_options: Dict[str, Any]) -> zarr.storage.StoreLike:
    """
    Open a Zarr store based on the provided URL and options
    :param zarr_url:
    :param type:
    :param kwargs:
    :return:
    """
    store_url = store_options['STORE_URL']  # The only mandatory parameter
    if re.match(r'^s3://', store_url):
        s3_opts = _s3_options(store_options)
        fs = fsspec.filesystem('s3', **s3_opts)
        # Remove s3:// scheme from path for FsspecStore
        clean_path = store_url.replace('s3://', '')
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=".*fs .* was not created with `asynchronous=True`.*",
                category=ZarrUserWarning,
            )
            return zarr.storage.FsspecStore(fs, path=clean_path)
    elif store_url.startswith('zip://'):
        store_url = store_url[len('zip://'):]
        return zarr.storage.ZipStore(store_url)
    else:
        if store_url.startswith('file://'):
            store_url = store_url[len('file://'):]
        path = Path(store_url)
        if not path.is_absolute():
            path = store_options.get('WORKDIR', ".") / path
        return zarr.storage.LocalStore(path)


def _zarr_fuse_options(schema: Optional[zarr_schema.NodeSchema], **kwargs) -> Dict[str, Any]:
    """
    Prepare options for ZarrFuse:
    - get kwargs
    - overwrite by schema ATTRS is provided
    - overwrite by evironment variables
    """
    interpreted_attrs = {'STORE_URL', 'S3_ENDPOINT_URL','S3_OPTIONS', 'WORKDIR', 'MODE'}
    secret_attrs = {'S3_ACCESS_KEY', 'S3_SECRET_KEY'}
    interpreted_attrs = interpreted_attrs.union(secret_attrs)
    options = {key:kwargs[key] for key in interpreted_attrs if key in kwargs}

    if schema is not None:
        schema_options = {key:schema.ds.ATTRS[key] for key in interpreted_attrs if key in schema.ds.ATTRS}
        # Warning for leaking secrets
        for key in secret_attrs:
            if key in schema_options:
                schema.ds.warn(f"Possible secrete leak in schema.", subkeys=['ATTRS', 'key'])
        options.update(schema_options)

    e_key = lambda key: f"ZF_{key}"  # Environment variable key prefix
    env_options = {key:os.environ[e_key(key)] for key in interpreted_attrs if e_key(key) in os.environ}
    options.update(env_options)
    return options

def _get_schema_safe(schema):
    if isinstance(schema, NodeSchema):
        return schema
    if isinstance(schema, str):
        if schema == '':
            return NodeSchema.make_empty()
        schema = Path(schema)
    if isinstance(schema, Path):
        return zarr_schema.deserialize(schema)
    else:
        raise TypeError(f"Unsupported schema type: {type(schema)}. Expected NodeSchema or Path.")


def _wipe_store(store):

    # For fsspec-style filesystems
    try:
        fs = store.fs
        # fs.rm(path, recursive=True) recursively deletes files and directories
        #fs.rm(root, recursive=True)
    except AttributeError:
        # Fallback to using os if fs is a local filesystem that lacks rm
        import os
        import shutil
        root = store.root
        # If root is nested, attempt to fully delete the directory
        if os.path.isdir(root):
            shutil.rmtree(root)
        elif os.path.exists(root):
            os.remove(root)
        return

    root = store.path  # Relative path inside the filesystem
    # Remove any trailing slash for consistency
    root = root.rstrip('/')

    # Ensure a client/session exists for sync wrappers (s3fs provides connect(); harmless if not present)
    try:
        fs.connect(refresh=False)  # sync twin of set_session()
    except Exception:
        pass

    # For asynchronous filesystems like AsyncS3FileSystem,
    # the fs.rm returned a coroutine; we need to run it on fsspec's loop
    loop = fsspec.asyn.get_loop()
    session = fsspec.asyn.sync(loop, fs.set_session, refresh=True)
    try:
        fsspec.asyn.sync(loop, fs._rm, root, recursive=True)
    except FileNotFoundError:
        pass
    finally:
        fsspec.asyn.sync(loop, session.close)
        type(fs).clear_instance_cache()

def remove_store(schema: zarr_schema.NodeSchema | Path, **kwargs):
    node_schema = _get_schema_safe(schema)
    options = _zarr_fuse_options(node_schema, **kwargs)
    try:
        store = _zarr_store_open(options)
    except ZFOptionError as e:
        raise ZFOptionError(f"{str(e)}. Opening store for schema {schema}.")

    _wipe_store(store)
    #store.delete_dir("")


def open_store(schema: zarr_schema.NodeSchema | Path | str, **kwargs):
    """
    Open existing or create a new ZARR store according to given schema.
    'schema': Could be schema dict or YAML string or Path object to YAML file.
              Could be None just for opening and existing storage.

    Kwargs are overwritten by schema 'ATTRS' values, which are
    overwritten by the environment variables named 'ZF_<ATTR_NAME>'.

    Return: root Node
    """
    node_schema = _get_schema_safe(schema)
    options = _zarr_fuse_options(node_schema, **kwargs)

    try:
        store = _zarr_store_open(options)
    except ZFOptionError as e:
        raise ZFOptionError(f"{str(e)}. Opening store for schema {schema}.")

    mode = options.get('MODE', 'a')
    return Node("", store, new_schema = node_schema, mode=mode)

class Node:
    """
    A lazy node representing a dataset stored in a single Zarr storage.
    We flatten the tree structure representing every node by a key equal to the node's path.
    (Note: if the length of the ptah would be the problem, we can have a dict attribute asigning path -> hash)
    Each path starts with and is separated by "/", root node name is "/".

    Each node’s dataset is stored under a group defined by a "/"‑separated relative path.

    Specific DataArray as Coord attributes:
    - chunk_size: int
    - description: str
    - unit: str (interpretable by Pint)
    - df_cols: List[str] (column names in the DataFrame)

    Specific DataArray as Quantity attributes:
    - description: str
    - unit: str (interpretable by Pint)
    - df_cols: List[str] (column names in the DataFrame)

    TODO:
    -  merge of schema:
        - new nodes ok
        - change in ATTRS OK
        - adding vars ok
        - adding coords ok
        - distinguisn:
            - parts that affects the storage (VARS, COORDS)
            - parts that affects Metadata
            - change in data acquisition
        -  possible: renamning of vars and coords
        -  impossible: change in vars coords
                       change in chuning
    - uddate dataset accoridng to the new schema
    - createion of storage if there is no schema
    """

    PATH_SEP = "/"


    @classmethod
    def read_store(cls, zarr_store):
        """
        Reconstruct the tree from a single Zarr store.

        Parameters:
          storage_url (str): The URL or path to the Zarr storage.

        Returns:
          Node: The root node of the reconstructed tree.

        Assumes that the root node is stored at the root group (i.e. with group path "").
        """
        root = cls("", store=zarr_store)
        return root


    @staticmethod
    def _variable(var: zarr_schema.Variable, data: np.ndarray, coords_dict: Dict[str, Any]) -> xr.Variable:
        """
        Create an xarray Variable from `data` array and variable schema.
        All xarray variables should be created this way to ensure structure.
        """
        coord_names = set(coords_dict.keys())
        for coord in var.coords:
            assert coord in coord_names, f"Variable {var.name} has unknown coordinate {coord}."
        #shape = tuple(0 for coord in var.coords)
        try:
            xr_var = xr.Variable(
                     dims=list(var.coords),  # Default dimension named after the coordinate key.
                     data=data,
                     attrs=var.zarr_attrs()
                 )
        except ValueError as e:
            raise ValueError(f"Variable '{var.name}' has incompatible shape {data.shape} for coordinates {var.coords}.") from e
        return xr_var


    @staticmethod
    def _coord_variable(name: str, coord: zarr_schema.Coord, var) -> xr.Variable:
        # simple coordinate
        np_var = np.array(var)
        if len(np_var.shape) > 1:
            raise ValueError(f"Dimension {coord.name}: coords array is not 1D. Has shape: {np_var.shape}.")

        unique, counts = np.unique(np.var, return_counts=True)
        # Find values that appear more than once
        repeated = unique[counts > 1]
        # Check if array has any non-unique values
        if repeated.size > 0:
            raise ValueError(f"Dimension {coord.name}: coords array has non-unique values: {repeated}.")

        np_var = xr.Variable(
            dims=(name,),
            data=np_var,
            attrs=coord.zarr_attrs()
        )
        return np_var

    @staticmethod
    def _create_coords(coords: Dict[str, zarr_schema.Coord], vars: Dict[str, xr.Variable]) -> xr.Coordinates:
        """
        Create xarray Coordinates from a dictionary of Coord schemas
        and objects using
        the explicit Coordinates constructor. Each coordinate is stored as an xarray.Variable.

        If a Coord has no `values`, an empty NumPy array is used.
        The chunk_size attribute is also stored.

        Parameters:
            coord_dict (Dict[str, Coord]): Mapping of coordinate names to Coord objects.

        Returns:
            Coordinates: An xarray Coordinates object built from the provided variables.
        """
        coord_vars = {
            name: Node._coord_variable(name, coord_schema, vars.get(name, []))
            for name, coord_schema in coords.items()
        }
        return xr.Coordinates(coord_vars)

    @staticmethod
    def empty_ds(schema: DatasetSchema) -> xr.Dataset:
        """
        Create an *empty* Zarr storage for multiple indices given by index_col.
        - first index is dynamic, e.g. time, new times are appended by update function
        - other indices are fixed, e.g. location, new locations are not allowed, all locations must be provided
          by the passed DF. However
           !! Need some mean to trace unused indices, relying on placehloders not possible for general types.

        - 'df' is used ONLY for column names/types (besides 'time_stamp' and 'location').
        - index_cols, [time_col, ...]
        - 'max_locations' is the fixed size for the location dimension.
        - idx_ranges reserved limits for each index column, the first index is just initali limit, could be appended.
        - Store an empty location_map in dataset attributes.
        - **kwargs can include chunking or compression settings passed to to_zarr().
          ... chunking coud either go to ds.chunks(...) or thrugh kwargs['enconding']['time'] = {'chunksizes': (1000, 10)}

        df, index_cols, iterators
        """
        # Identify the data columns (excluding time_stamp, location)

        # Build an Xarray Dataset with dims: (time=0, location=max_locations)
        # We'll create a coordinate for "time" (initially empty),
        # and a coordinate for "location" (just an integer range 0..max_locations-1).

        variables = {}
        coords_obj = {}
        attrs = {
            '__structure__': zarr_schema.serialize(schema),
            '__empty__': True
        }
        attrs.update(schema.ATTRS)
        ds = xr.Dataset(
            data_vars=variables,
            coords=coords_obj,
            attrs=attrs
        )
        return ds




    def __init__(self, name, store, parent=None,
                 new_schema:zarr_schema.NodeSchema=None,
                 mode="a"):
        """
        Parameters:
          name (str): The name of the node. For the root node, use an empty string ("").
          store (MutableMapping): The underlying Zarr store.
          parent (Node, optional): Parent node.
        """
        self.name = name
        self.store = store
        if store.read_only:
            mode = 'r'
        else:
            # Make sure group exists.
            zarr_root_dict = zarr.open_group(self.store).store

        self.mode = mode
        self.parent = parent

        self.children = self._make_consistent(new_schema)
        # This calls self._update_schema_ds() implicitely.


    @cached_property
    def logger(self):
        if self.mode == 'r':
            return RaisingLogger(get_logger(store=None, path=self.group_path))
        else:
            zarr_root_dict = zarr.open_group(self.store).store
            return get_logger(zarr_root_dict, self.group_path)




    def _make_null_ctx(self):
        return zarr_schema.SchemaCtx([], file='', logger=self.logger)

    def _storage_group_paths(self):
        """
        Read actual Node subgroups in storage.
        :return: list of subgoup names
        TODO: rename to '_read_subgroups'
        """
        path = self.group_path.strip(self.PATH_SEP)
        root_group = zarr.open_group(self.store, path=path, mode='r')
        sub_groups = list(root_group.groups())
        return sub_groups

    # def _add_node(self, name):
    #     assert name not in self.children
    #     node = Node(name, self.store, self)
    #     self.children[name] = node
    #     return node

    def _make_consistent(self, new_node_schema:zarr_schema.NodeSchema) -> Dict[str, 'Node']:
        """
        - merge given new_schema
        - recreate self.children from the storage
        - possibly introduce new nodes according to schema with empty_ds
        Still not completely safe as the storage can be changed by other process.
        TODO:
        - introduce kind of lock mechanism when changing DS schema
        - however only adding groups should be safe

        return children dict
        """

        if new_node_schema is None:
            # node in storage but not in schema
            # use ds_schema from the storage and empty new children dict
            null_address = self._make_null_ctx()
            new_node_schema = zarr_schema.NodeSchema(
                null_address,
                self.schema,
                {})  # empty groups

        self._update_schema_ds(new_node_schema.ds)

        child_node_names = set([key for key, _ in self._storage_group_paths()])
        child_node_names.update(new_node_schema.groups.keys())

        def make_child(key):
            try:
                new_child_schema = new_node_schema.groups[key]
            except KeyError:
                #
                self.logger.warning(f"Key {key} is missing in the new schema, keeping it.")
                new_child_schema = None

            # Here we do indirect recursion of _make_consistent.
            return Node(key, self.store, parent=self, new_schema=new_child_schema)

        # Process existing child Nodes
        childern = {
            key: make_child(key)
            for key in child_node_names
        }
        return childern

    def _update_schema_ds(self, new_schema:zarr_schema.DatasetSchema) -> zarr_schema.DatasetSchema:
        """
        Update DS according to new schema.
        :param new_schema:
        :return: updated_schema
        TODO:
        Currently we only support creating new DS.
        - Implement changes in schema not affecting the DS (descriptions, dims)
        - Implement changes in schema extending DS (new vars and coords)
        - Implement versioning and substantial DS changes.
        """
        try:
            old_schema = self.schema
        except (KeyError,GroupNotFoundError):
            cfg = zarr_schema.ContextCfg(dict(attrs={}, vars={}, coords={}), self._make_null_ctx())
            old_schema = zarr_schema.DatasetSchema(cfg['attrs'], cfg['vars'], cfg['coords'])

        if old_schema.is_empty():
            # Initialize new dataset.
            empty_ds = Node.empty_ds(new_schema)
            self._init_empty_grup(empty_ds)
            assert self.schema == new_schema
            return new_schema
        else:
            # Preserve dataset schema.
            assert new_schema == old_schema, (f"Modifying node dataset schema is not supported."
                                      f"\nold:{old_schema}\nnew{new_schema} ")
            return old_schema

    def items(self):
        return self.children.items()

    def __getitem__(self, key):
        return self.children[key]

    @property
    def root(self):
        """
        Return the root node of the tree.
        """
        if self.parent is None:
            return self
        else:
            return self.parent.root

    @property
    def _path_list(self):
        """
        Return node address as a list of keys.
        :return:
        """
        if self.parent is None:
            return []
        else:
            return [*self.parent._path_list, self.name]

    @property
    def group_path(self):
        """
        Compute the group path for this node from its parent's group_path and its name.
        For the root node (parent is None), returns "" (empty string).
        For child nodes, returns a string like "child" or "parent/child".
        """
        return Node.PATH_SEP.join(self._path_list)

    @property
    def dataset(self):
        """
        Lazily open the dataset stored in this node's group.
        Returns a (possibly dask‑backed) xarray.Dataset.
        """
        rel_path = self.group_path #+ self.PATH_SEP + "dataset"
        rel_path = rel_path.strip(self.PATH_SEP)
        ds = xr.open_zarr(self.store, group=rel_path, consolidated=False)
        for coord in ds.coords:
            assert  'composed' in ds.coords[coord].attrs
        return ds

    @property
    def schema(self) -> zarr_schema.DatasetSchema:
        """
        Return node dataset schema.
        I.e. dictionary with keys 'COORDS', 'VARS', 'ATTRS'.
        Original schema tree is spread over the storage groups represented by Nodes.
        :return:
        """
        node_schema = zarr_schema.deserialize(
            self.dataset.attrs['__structure__'],
            source_description='<storage schema>'
        )
        return node_schema.ds


    def export_schema(self):
        """
        Return schema of whole storage under 'self' Node.
        :return:
        """
        pass

    def update(self, polars_df):
        """
        Atomically update this node's dataset using a Polars DataFrame.

        Parameters:
          polars_df: A Polars DataFrame (or substitute) with new data.
          dataset_update_func: A function taking (dataset, polars_df) and returning an updated dataset.

        The updated dataset is written (overwriting) to the Zarr group.
        TODO:
        - loop update approach modified using:
           - pivot_nd for DF -> DS conversion, should be a method that uses coords and df_cols
           -
        """
        ds = pivot_nd(self.schema, polars_df, self.logger)
        written_ds, merged_coords = self.merge_ds(ds)
        # check unique coordsregion="auto",
        dup_dict = check_unique_coords(written_ds)
        if  dup_dict:
            self.logger.error(dup_dict)
        #return written_ds

    def update_dense(self, vars):
        # TODO:
        # Allow automatic adding of coordinates only for coordinates with values
        # provided as part of schema.
        # for coord_name, _ in self.schema.COORDS.items():
        #     if coord_name not in vars:
        #         # Assume update over full span of coordinates that are not
        #         # explicitely in the vars dict.
        #         vars[coord_name] = self.dataset[coord_name].values()

        ds = dataset_from_np(self.schema, vars)
        written_ds, merged_coords = self.merge_ds(ds)
        # check unique coordsregion="auto",
        dup_dict = check_unique_coords(written_ds)
        if dup_dict:
            self.logger.error(dup_dict)
        #return written_ds

    def _validate_ds_against_schema(self, ds: xr.Dataset):
        """
        Ensure that a provided xarray.Dataset is compatible with this node's schema:
        - all schema coordinates are present,
        - all schema variables are present,
        - variable dims match the schema's coord list.

        Extra coordinates/variables in `ds` are allowed but logged as a warning.
        """
        schema = self.schema
        path_str = self.group_path or "/"

        # --- Coordinates ---
        for cname, c_schema in schema.COORDS.items():
            c_schema.validate_ds_coord(ds.coords.get(cname, None), path_str, self.logger)
            if cname not in ds.coords:
                self.logger.error(
                    KeyError(f"Source dataset is missing coordinate '{cname}'."))

            coord = ds.coords[cname]
            # In your own code you always create coords as 1D with dim == name
            if coord.ndim != 1 or coord.dims != (cname,):
                raise ValueError(
                    f"Coordinate '{cname}' for node '{path_str}' must be 1D with "
                    f"dimension '{cname}', got dims={coord.dims}."
                )

        # --- Variables ---
        for vname, vschema in schema.VARS.items():
            if vname not in ds.data_vars:
                raise ValueError(
                    f"Dataset for node '{path_str}' is missing variable '{vname}' "
                    f"required by schema."
                )

            da = ds[vname]
            expected_dims = tuple(vschema.coords)
            if tuple(da.dims) != expected_dims:
                raise ValueError(
                    f"Variable '{vname}' in node '{path_str}' has dims {da.dims}, "
                    f"but schema expects {expected_dims}."
                )

            # ensure all dims exist as coordinates in schema
            for dim in da.dims:
                if dim not in schema.COORDS:
                    raise ValueError(
                        f"Variable '{vname}' in node '{path_str}' uses dimension "
                        f"'{dim}' which is not declared as a coordinate in schema."
                    )

        # --- Warn about extras (but allow them) ---
        extra_vars = set(ds.data_vars) - set(schema.VARS)
        if extra_vars:
            self.logger.warning(
                f"Node '{path_str}': dataset has variables not present in schema: "
                f"{sorted(extra_vars)}. They will still be written."
            )

        extra_coords = set(ds.coords) - set(schema.COORDS)
        if extra_coords:
            self.logger.warning(
                f"Node '{path_str}': dataset has coordinates not present in schema: "
                f"{sorted(extra_coords)}. They will still be written."
            )

    def update_from_ds(self, ds: xr.Dataset):
        """
        Alternative to `update_dense` that accepts a fully-formed xarray.Dataset.

        - Verifies that `ds` is compatible with this node's schema
          (coordinates + variables).
        - Then merges it into the underlying zarr store via `merge_ds`.

        This is intended for use by higher-level tools (e.g. `zf cp`) that
        already have an xarray.Dataset and don't need the Polars/NumPy path.
        """
        # Validate compatibility with the current node schema
        self._validate_ds_against_schema(ds)

        # Merge/write to the store using existing logic
        written_ds, merged_coords = self.merge_ds(ds)

        # Optional: still check for duplicated coordinates and log them
        dup_dict = check_unique_coords(written_ds)
        if dup_dict:
            self.logger.error(dup_dict)

        return written_ds
    def _init_empty_grup(self, ds):    # open (or create) the root Zarr group in “write” mode
        rel_path = self.group_path  # + self.PATH_SEP + "dataset"
        rel_path = rel_path.strip(self.PATH_SEP)

        # Create the group if it does not exist.
        grp = zarr.open_group(self.store, path=rel_path, mode="a")

        # ds.to_zarr does not write the attrs, either because of mode != "w" or
        # due to non-consolidated metadata.
        # Temporary workaround until we use consolidated metadata:
        # works as long as we do not allow to modify dataset schema
        grp.attrs.update(ds.attrs)
        written_ds = self.write_ds(ds, mode="r+", region="auto")
        assert '__structure__' in written_ds.attrs
        return written_ds

    def write_ds(self, ds, **kwargs):
        rel_path = self.group_path # + self.PATH_SEP + "dataset"
        rel_path = rel_path.strip(self.PATH_SEP)
        #path_store = zarr.open_group(self.store, mode=mode, path=rel_path)
        #ds.to_zarr(path_store,  **kwargs)
        ds.to_zarr(self.store, group = rel_path, consolidated=False, **kwargs)

        # written_ds = xr.open_zarr(self.store, group=rel_path)
        # assert '__structure__' in ds.attrs
        # assert '__structure__' in written_ds.attrs
        return ds

    """
     For the updating DF we define "overlap" slice:
     For sorted coord:  current_coords > new_coords.min() 
     For unsorted coords: current_coord_idx > index_of( argmin( current_coords _intersect_ new_coords))
     However new_coords could undergo e.g. projection to existing coords.
     How to unify the procedure?
    """



    def merge_ds(self, ds_update: xr.Dataset) -> xr.Dataset:
        """
        Merge xarray dataset `ds_update` into the node dataset in the zarr storage.
        The `ds_update` must be subarray of the storage dataset.

        This function works in two phases:

        Phase 1 (Dive):
          For each dimension in dims_order (in order):
            - Split ds_update along that dimension into:
                • overlap: coordinate values that already exist in the store.
                • extension: new coordinate values.
            - Save the extension subset (per dimension) for later appending.
            - For subsequent dimensions, keep only the overlapping portion.

        Phase 2 (Upward):
          - Write the final overlapping subset using region="auto".
          - Then, in reverse order, for each dimension that had an extension:
                • Reindex the corresponding extension subset so that for all dimensions
                  except the current one the coordinate values come from the store.
                • Append that reindexed subset along the current dimension.
                • Update the merged coordinate for that dimension.

        Modular updating mechanism:
        - interpolate step: interpolate updating DF coords to existing coords
          (postponed), regularize or constrain coord step etc.
          each coord interpolates independently, according to their order in the var list
          Coord interpolator is a (sparse) matrix mapping values in updating coords to existing coords.
        - appendable: True (default) / False [ Set to False to disallow new coord values after first update (creation)]
          Only for check purposes, no effect on resulting values.
        - sorted: [list of vars to sort by], [] do not sort, None (default) (automatic, sort by itself for scalar coords, unsorted for composed)
          Sorted coords imply merge of the values leading to larger overlap/overwrite region.

        Parameters
        ----------
        zarr_path : str
            Path to the existing Zarr store.
        ds_update : xr.Dataset
            The dataset to update. Its coordinate values in one or more dimensions may be new.
        dims_order : list of str, optional
            The list of dimensions to process (in order). If None, defaults to list(ds_update.dims).

        Returns: (updated DataSet after write, merged_coords)
        Are merged_coords still necessary?

        New procedure:
        1. interpolate ds_new, using ds_existing coords (replaces Phase 1)
            a) each coord detemine overlap range (indices)
            b) detemine extended coords (no insertions) variants: None, all new, limited step, fixed step
        2. interpolate overlap, write it.

           However interpolation of whole ds_new should rather be done as we possibly change the new coords as well.

           interpolate variables to sorted coords (interpolation of unsorted but sparse coords possible in future
               Due to nans this could be problematic without having also existing data.
               The interpolation takes place only in the overlaping part, that is intersection of overlaps in all dimensions.

        3. write extended / interpolated parts (Phase 2)
        """
        ds_existing = self.dataset

        # --- Phase 1: Dive (split by dimension) ---
        # We create a dict to hold the extension subset for each dimension.
        if '__empty__' in ds_existing.attrs:
            del ds_existing.attrs['__empty__']
            return self.write_ds(ds_update, mode="a"), {}

        ds_update, split_indices = interpolate_ds(
            ds_update,
            self.dataset,
            self.schema.COORDS)
        last_written_ds = None
        ds_extend_dict = {}
        ds_overlap = ds_update.copy()
        dims_order = tuple(ds_update.coords.keys())
        for dim, idx in split_indices:
            ds_extend_dict[dim] = ds_overlap.isel({dim: slice(idx, None)})
            ds_overlap = ds_overlap.isel({dim: slice(0, idx)})

        # At this point, ds_overlap covers only the coordinates that already exist in the store
        # in every dimension in dims_order. Write these (overlapping) data using region="auto".
        update_overlap_size = np.prod(list(ds_overlap.sizes.values()))
        if update_overlap_size > 0:
            last_written_ds = self.write_ds(ds_overlap, mode="r+", region="auto")

        # --- Phase 2: Upward (process extension subsets in reverse order) ---
        # We also update a merged_coords dict from the store.
        merged_coords = {d: ds_existing[d].values for d in ds_existing.dims}

        # Loop upward in reverse order over dims_order.
        for dim in reversed(dims_order):
            ## merged = ds1.combine_first(ds2).sortby("dim")

            dim_coord = ds_extend_dict[dim]
            if dim_coord is None or dim_coord.sizes.get(dim, 0) == 0:
                continue  # No new coordinates along this dimension.

            # For all dimensions other than dim, reindex ds_ext so that the coordinate arrays
            # come from the store (i.e. the full arrays). This ensures consistency.
            # (This constructs an indexers dict using the existing merged coordinates.)
            indexers = {d: merged_coords[d] for d in dim_coord.dims if d != dim}
            na_value = ds_update.attrs.get('na_value', np.nan)
            ds_ext_reindexed = dim_coord.reindex(indexers, fill_value=na_value)

            # Append the extension subset along the current dimension.
            last_written_ds = self.write_ds(ds_ext_reindexed, mode="a", append_dim=dim)

            # Update merged coordinate for dim: concatenate the old coords with the new ones.
            new_coords_for_dim = dim_coord[dim].values
            merged_coords[dim] = np.concatenate([merged_coords[dim], new_coords_for_dim])

        assert last_written_ds is not None, "No data was written to the dataset."
        return last_written_ds, merged_coords

    """
    Good, now how to extend the code to two and more dimensions?
I need something like:

split_dict = {}
dim_order = list(ds_zarr.coords.keys())
for dim in dim_order:
    n, merged_new_coords = merge_coords(dim, new_ds.coords[dim])
    l_overlap = len(ds_zar.coords[dim]) - N
    split_dict[dim] = (n,     l_overlap, merged_new_coords)
 
dim = dim_order[0]
` pad ds_zarr by Nans over [0:N] in 'dim', adding len(new_coords) - l_overlap for each d > dim'
N, L, coords = slpit_dir[dim]
ds_zarr_tail = ds_zarr.isel({dim: slice(N, None)}).copy()
ds_update.combine_first(ds_zarr_tail).sortby(dim)
    """

    # def read_df(self, var_name, *args, **kwargs):
    #     """
    #     Read a multidimensional sub-range from self.dataset[var_name] and convert it to a Polars DataFrame.
    #
    #     Parameters
    #     ----------
    #     var_name : str
    #         The name of the variable in the dataset to extract.
    #     *args, **kwargs :
    #         Additional arguments passed to the xarray selection method (defaulting to .sel).
    #         For example, use keyword arguments to specify coordinate ranges:
    #             read_df("temperature", time=slice("2025-01-01", "2025-01-31"))
    #
    #     Returns
    #     -------
    #     pl.DataFrame
    #         A Polars DataFrame containing the subset data, with coordinate information included as columns.
    #     """
    #     # Get the DataArray from the dataset
    #     da = self.dataset[var_name]
    #
    #     # Extract the desired subset.
    #     # You can change .sel to .isel if you prefer index-based selection.
    #     sub_da = da.sel(*args, **kwargs)
    #
    #     # Convert the subset to a DataFrame.
    #     # This will put coordinate information in the index, so we reset the index.
    #     pd_df = sub_da.to_dataframe().reset_index()
    #
    #     # Convert the Pandas DataFrame to a Polars DataFrame.
    #     return pl.from_pandas(pd_df)

    def read_df(self, var_names: Union[str, List[str]], *args, **kwargs):
        return Node._read_df(self.dataset, var_names, *args, **kwargs)

    @staticmethod
    def _read_df(ds: xr.Dataset, var_names: Union[str, List[str]], *args, **kwargs) -> Tuple[pl.DataFrame, List[str]]:
        """
        Read one or more variables from self.dataset (with coords), convert to a Polars DataFrame,
        and return that DF along with the list of all coordinate 'dims'.

        Parameters
        ----------
        var_names : str or list of str
            Name(s) of the variable(s) in the dataset to extract.
        *args, **kwargs :
            Passed through to xarray's `.sel` (or `.isel` if you swap it).
            E.g. read_df("temp", time=slice("2025-01-01","2025-01-10"))
                 read_df(["u","v"], lon=slice(0,10), lat=slice(-5,5))

        Returns
        -------
        df : pl.DataFrame
            Polars DataFrame of the selected data; coords become columns too.
        dims : List[str]
            A flattened list of all dimension names used by the coordinate variables
            in the returned slice.
        """
        # 1. Normalize var_names → list
        if isinstance(var_names, str):
            var_list = [var_names]
        else:
            var_list = list(var_names)


        # Get all dimmensions, even composed.
        dims = set()
        for coord in ds.coords:
            print(f"Coord: {coord}, {ds.coords[coord].dims}")
            dims = dims.union(set(ds.coords[coord].attrs['composed']))

        print(f"Read DF: {var_list}, dimss: {dims}")
        var_list = var_list + list(dims)
        # 2. Pull either a DataArray (single var) or Dataset (multiple vars)
        ds_vars = ds[var_list]

        # 3. Subset by coordinates/index
        sub = ds_vars.sel(*args, **kwargs)

        # 4. To pandas → reset index → to polars
        pd_df = sub.to_dataframe().reset_index()
        pl_df = pl.from_pandas(pd_df)

        # 5. Collect all coord dims

        return pl_df


def check_unique_coords(ds):
    """
    Check that all coordinate rows are unique.

    Parameters
    ----------
    coords : sequence of tuples or numpy.ndarray of shape (n, m)
        The dataset of coordinates to check.

    Returns
    -------
    dict
        A mapping from each duplicated coordinate to the list of indices
        at which it appears. If empty, all coords are unique.
    """
    def duplicities(arr):
        unique_vals, counts = np.unique(arr, return_counts=True)
        return arr[counts > 1]

    return {
        name: dup_vals
        for name, coord in ds.coords.items()
        if (dup_vals := duplicities(coord.values)).size > 0
    }


def eliminate_dims_if_equal(arr: np.ndarray, dims_to_check: List[bool]) -> np.ndarray:
    """
    Check that for each axis flagged True in dims_to_check, all values along that axis
    are equal. For each such axis, eliminate that dimension by taking the 0-th slice.

    Parameters
    ----------
    arr : np.ndarray
        Input array.
    dims_to_check : tuple of bool
        A tuple of booleans, one per dimension of arr. For each dimension where the
        value is True, the function will check that all values along that axis are equal
        and then remove that axis.

    Returns
    -------
    np.ndarray
        A new array with the constant axes removed.

    Raises
    ------
    ValueError
        If dims_to_check length does not match arr.ndim, or if any flagged axis does not
        have all equal values.
    """
    if arr.ndim != len(dims_to_check):
        raise ValueError("Length of dims_to_check must equal number of dimensions in arr")

    # Process axes in descending order so that removal of one axis doesn't change indices.
    for axis in reversed(range(arr.ndim)):
        if dims_to_check[axis]:
            # Take the first element along the axis.
            ref = np.take(arr, 0, axis=axis)
            # Expand dims so that it can broadcast for comparison.
            ref_expanded = np.expand_dims(ref, axis=axis)
            if not np.all(arr == ref_expanded):
                print(arr)
                raise ValueError(f"Values along axis {axis} are not all equal")
            # Remove this axis by selecting the 0th element along that axis.
            arr = np.take(arr, 0, axis=axis)
    return arr

def get_df_col(df, var:zarr_schema.Variable, logger: logging.Logger):
    try:
        col_series = df[var.df_col].to_numpy()
    except pl.exceptions.ColumnNotFoundError:
        logger.error(f"Source column '{var.df_col}' not found in the input DataFrame:\n{df.head()}")
        return np.full(df.shape[0], var.na_value)
    try:
        q_new = var.convert_values(col_series)
    except ValueError as e:
        # TODO: better test of the var.convert_values and treatment of possible conversion error there
        # reporting detailed variable info.
        print(col_series)
        logger.error(f"Failed to parse values of column '{var.df_col}' with\n"
                     f"unit/format specification:{var.source_unit}\n"
                     f"values:\n{df.head()}"
                     f"\n Original error: {e}")
        return np.full(df.shape[0], var.na_value)
    return q_new

        # TODO: log missing column


def dataset_from_np(schema: zarr_schema.DatasetSchema, vars: Dict[str, np.ndarray]) -> xr.Dataset:
    """
    Create an xarray.Dataset from a schema and a dictionary of NumPy arrays.
    :return:
    """
    coords_dict = Node._create_coords(schema.COORDS, vars)
    data_vars = {k: Node._variable(schema.VARS[k], var_np_array, coords_dict)
                 for k, var_np_array in vars.items()
                 if k not in coords_dict}
    attrs = schema.ATTRS
    attrs['__structure__'] = zarr_schema.serialize(schema)
    ds_out = xr.Dataset(data_vars=data_vars, coords=coords_dict, attrs=attrs)
    return ds_out


def coerce_df(schema: zarr_schema.DatasetSchema,
              df: pl.DataFrame,
              logger):
    """
    Front half of pivot_nd:

    1. Coerce all variable columns from df into 1D numpy arrays (no masking yet).
    2. Coerce all coord columns (including composed coords) into data_vars,
       then build coordinate arrays per dimension.
    3. Compute per-row integer indices into each coord axis, and a valid_rows
       mask (rows with valid coords).
    4. Build df_multi_idx (tuple of index arrays) for valid rows and apply
       valid_rows to variable columns.
    5. Build final coords_dict via Node._create_coords.

    Returns
    -------
    df_multi_idx : tuple[np.ndarray, ...]
        Multi-index: one 1D index array per dim, all length K_valid.

    coords_dict : dict[str, Any]
        Final coordinate mapping as produced by Node._create_coords.

    var_data : dict[str, np.ndarray]
        Variable columns from df, coerced and masked by valid_rows.
    """
    # --- Step 1: DF -> dict of 1d arrays (variables only), no masking yet ---
    data_vars = {
        k: get_df_col(df, var, logger)
        for k, var in schema.VARS.items()
    }

    # --- Step 2: apply hash of tuple coords / coerce coord columns ---
    # (kept very close to your original code)
    valid_rows = np.ones(len(df), dtype=bool)
    for k, c in schema.COORDS.items():
        if (c.composed is not None) and (len(c.composed) > 1):
            # hash tuple coords
            tuple_list = zip(*(data_vars[comp] for comp in c.composed))
            coord_valid_rows = np.all([schema.VARS[comp].valid_mask(data_vars[comp]) for comp in c.composed], axis=0)
            hash_list = [hash(tuple(t)) for t in tuple_list]
            data_vars[k] = np.array(hash_list, dtype=np.int64)
        else:
            # mix vars and coord to be backward compatible with remaining code
            col = get_df_col(df, c, logger)
            coord_valid_rows = c.valid_mask(col)
            data_vars[k] = col
            # print("1D coord:", k)
        valid_rows = valid_rows & coord_valid_rows
    # filter data_vars
    data_vars = {k:v[valid_rows] for k,v in data_vars.items()}

    # --- Step 3: extract coords and build indices / valid_rows ---
    idx_list = []
    coords_dict_raw = {}
    dims = list(schema.COORDS.keys())

    #n_rows = len(df)
    #valid_rows = np.ones(n_rows, dtype=bool)

    for d in dims:
        df_coord_array = data_vars[d]

        coords = np.unique(df_coord_array)
        coords = coords[schema.COORDS[d].valid_mask(coords)]
        coords = np.sort(coords)

        coords_dict_raw[d] = coords
        final_idx = np.searchsorted(coords, df_coord_array)
        idx_list.append(final_idx)

    # Multi-index: one index array per dim, but only for valid rows
    #df_multi_idx = tuple(idx[valid_rows] for idx in idx_list)
    # multiindex for each df row, but flattend, so it actually index result_nd_array.flat[..]
    coord_sizes = [len(coords_dict_raw[d]) for d in dims]
    df_multi_idx = np.ravel_multi_index([idx[valid_rows] for idx in idx_list], dims=coord_sizes)  #

    # Apply valid_rows to the variable columns from Step 1
    var_data = {k: col[valid_rows] for k, col in data_vars.items() if k not in coords_dict_raw}

    # --- Step 5: let Node build final coords (xarray-ready etc.) ---
    coords_dict = Node._create_coords(schema.COORDS, coords_dict_raw)

    return df_multi_idx, coords_dict, var_data


def pivot_nd(schema:zarr_schema.DatasetSchema, df: pl.DataFrame, logger):
    """
    Pivot a Polars DataFrame with columns for each dimension in self.dataset.dims
    and one value column (per variable) into an N-dim xarray.Dataset.

    For each dimension, the coordinate values are taken as the intersection between
    the dataset’s coordinate (self.dataset[d].values) and the unique values in df
    (from the column specified by self.dataset[d].attrs['df_cols'][0]). The intersection
    is done in a vectorized way and preserves the order given by the dataset coordinate.

    For each variable in self.dataset.variables, values from the corresponding df column
    are inserted into an output array of shape determined by the sizes of the common
    coordinate sets. If duplicate keys occur, later values win.

    Returns:
        xr.Dataset: The pivoted dataset with data variables defined on the new N-dim grid,
                    and with coordinates for each dimension.

    TODO: Review and simplify for clearly separated vars and coords.
    """
    # # 1. DF -> dict of 1d arrays,
    # data_vars = {
    #     k: get_df_col(df, var, logger)
    #     for k, var in schema.VARS.items()
    # }
    # # 2. apply hash of tuple coords, input Dict: ds_name : [df_cols]
    # for k, c in schema.COORDS.items():
    #
    #     if (c.composed is not None) and (len(c.composed) > 1):
    #         # hash tuple coords
    #         tuple_list = zip( *(data_vars[c] for c in c.composed) )
    #         hash_list = [hash(tuple(t)) for t in tuple_list]
    #         data_vars[k] = np.array(hash_list, dtype=np.int64)
    #         #print("Composed coord:", k)
    #         #print(data_vars[k])
    #
    #     else:
    #         # mix vars and coord to be backward compatible with remaining code
    #         col = get_df_col(df, c, logger)
    #         data_vars[k] = col
    #         #print("1D coord:", k)
    #         #print(data_vars[k])
    #
    # # 3. Extract coords
    # idx_list = []
    # coords_dict = {}
    # dims = list(schema.COORDS.keys())
    # # Loop over each dimension in the original dataset.
    # valid_rows = np.ones_like(len(df), dtype=bool)
    # for d in dims:
    #     # Get the name(s) of the column(s) in df corresponding to this dimension.
    #     df_coord_array = data_vars[d]
    #     # Get the coordinate values from the dataset (assumed to be in desired order).
    #     #print(d)
    #     #print(df_coord_array)
    #     coords = np.unique(df_coord_array)
    #     coords = coords[schema.COORDS[d].valid_mask(coords)]
    #     coords = np.sort(coords)
    #     # TODO: filter rows with NA coords first; follow with refactoring pivot_nd into distinguished steps
    #     # preparation to separation into tasks
    #     # In order to avoid special hash returning NA for composed coords with on NA value
    #
    #     coords_dict[d] = coords  # will be used as the coordinate values for this dim.
    #     #coord_sizes[d].append(len(coords))
    #     # Map each row’s coordinate (from df) to its index in the common_coords.
    #     # (This works as long as common_coords is sorted. In many cases ds coordinates are already sorted.)
    #     final_idx = np.searchsorted(coords, df_coord_array)
    #     valid_rows = valid_rows & (coords[final_idx] == df_coord_array)
    #     idx_list.append(final_idx)
    # coord_sizes = [len(coords_dict[d]) for d in dims]
    # # multiindex for each df row, but flattend, so it actually index result_nd_array.flat[..]
    # df_multi_idx = np.ravel_multi_index([idx[valid_rows] for idx in idx_list], dims=coord_sizes)  #
    # coords_dict = Node._create_coords(schema.COORDS, coords_dict)

    df_multi_idx, coords_dict, var_data = coerce_df(schema, df, logger)
    coord_sizes = [len(coord) for coord in coords_dict.values()]

    # 4. form variables
    # Helper: for a given variable, build the pivoted array.
    def form_var_array(var_struc):
        column_vals = var_data[var_struc.name]
        # Choose a dtype based on the column (floating or object)
        #dtype = column_vals.dtype if np.issubdtype(column_vals.dtype, np.floating) else object
        # Create an empty output array with fill_value.
        result = np.full(coord_sizes, var_struc.na_value, dtype=column_vals.dtype)

        mask_valid = var_struc.valid_mask(column_vals)

        # Fill in the values; if duplicate keys occur, later values win.
        result.flat[df_multi_idx[mask_valid]] = column_vals[mask_valid]
        # eliminate inappropriate coordinates
        dims_to_eliminate = [d not in var_struc.coords for d in coords_dict.keys()]
        np_var = eliminate_dims_if_equal(result, dims_to_eliminate)
        data_var = Node._variable(var_struc, np_var, coords_dict)
        return data_var

    # 4. form arrays (remaining vars)
    # Create DataArrays for each variable using the dims specified in self.dataset.
    data_vars = {k: form_var_array(var)
                 for k, var in schema.VARS.items()
                 if k not in coords_dict}

    # Build and return the xarray.Dataset with the computed coordinates.
    attrs = schema.zarr_attrs()
    attrs['__structure__'] = zarr_schema.serialize(schema)
    ds_out = xr.Dataset(data_vars=data_vars, coords=coords_dict, attrs=attrs)
    return ds_out

#
#
# def read(zarr_path: Path, time_stamp_slice, locations):
#     """
#     Read a subset of the data for a given time slice and a list of locations.
#
#     - time_stamp_slice: a tuple (start, end)
#     - locations: list of location codes.
#
#     Returns a Polars DataFrame with columns: time_stamp, location, and each data variable.
#     For each (time_stamp, location) pair in the slice, the stored value is returned.
#     """
#     ds = xr.open_zarr(str(zarr_path), chunks=None)
#     time_col = "time_stamp"
#     loc_col = "location"
#
#     time_coords = ds.coords[time_col].values
#     start, end = time_stamp_slice
#     mask = (time_coords >= start) & (time_coords <= end)
#     sel_time_idx = np.where(mask)[0]
#     sel_times = time_coords[sel_time_idx]
#
#     # Get the location map and select the slots corresponding to requested locations.
#     loc_map = json.loads(ds.attrs.get("location_map", "{}"))
#     selected_locs = {loc: loc_map[str(loc)] for loc in locations if str(loc) in loc_map}
#
#     data_cols = [col for col in ds.data_vars if col not in [time_col, loc_col]]
#     out_time = []
#     out_loc = []
#     out_data = {col: [] for col in data_cols}
#
#     for t_idx in sel_time_idx:
#         for loc, slot in selected_locs.items():
#             out_time.append(time_coords[t_idx])
#             out_loc.append(loc)
#             for col in data_cols:
#                 val = ds[col].values[t_idx, slot]
#                 out_data[col].append(val)
#     ds.close()
#
#     data_dict = {time_col: out_time, loc_col: out_loc}
#     data_dict.update(out_data)
#     return pl.DataFrame(data_dict)