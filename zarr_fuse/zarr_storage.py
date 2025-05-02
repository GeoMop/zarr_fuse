import inspect
import re
from typing import List, Callable, Dict, Optional, Set, Tuple, Union, Any
import polars as pl
import xarray as xr
import numpy as np
import zarr
from pathlib import Path
from collections import defaultdict

from . import zarr_schema
from .logger import StoreLogHandler

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
"""

def zarr_store_guess_type(zarr_url:Union[str, Path] = ""):
    """
    Guess and return an appropriate zarr store constructor based on the given storage path or URL.
    """
    # If no URL or empty string provided, default to an in-memory store.
    if zarr_url == "":
        return 'memory'

    # Check if the path ends with a .zip extension
    # EXPERIMENTAL
    if zarr_url.endswith(".zip"):
        return 'zip'

    if re.match(r'^[a-zA-Z0-9_]+://', zarr_url):
        return 'remote'

    # Otherwise, assume it's a local directory path.
    # Optionally, you might want to expand user (~) or environment variables here.
    return 'local'


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


def zarr_storage_open(zarr_url, type='guess', **kwargs):
    if type == 'guess':
        type = zarr_store_guess_type(zarr_url)

    if type == 'memory':
        args = (zarr.storage.MemoryStore, dict())
    elif type == 'local':
        if not zarr_url.startswith('/') and not zarr_url.startswith('file://'):
            zarr_url = kwargs.get('workdir', Path()) / zarr_url
        args = (zarr.storage.LocalStore, zarr_url)
    elif type == 'remote':
        args = (zarr.storage.FsspecStore, zarr_url)
    elif type == 'zip':
        args = (zarr.storage.ZipStore, zarr_url)
    storage = call_with_filtered_kwargs(*args, **kwargs)
    return storage

def open_storage(schema=None, **kwargs):
    """
    Open existing or create a new ZARR storage according to given schema.
    Function arguments overrides respective schema 'ATTRS' values.
    The schema can be either a dictionary or a path to a YAML file.

    'schema': Could be schema dict or YAML string or Path object to YAML file.
              Could be None just for opening and existing storage.

    kwargs processed:
    'workdir' used for relative local urls.
    Return: root Node
    """
    if isinstance(schema, dict):
        schema_dict = schema
    elif isinstance(schema, (str, Path)):
        schema_dict = zarr_schema.deserialize(schema)

    store_attrs = schema['ATTRS'].copy()
    store_attrs.update(kwargs)
    zarr_url = store_attrs['store_url']
    type = store_attrs.get('store_type', 'guess')
    kwargs.update(store_attrs)
    storage = zarr_storage_open(zarr_url, type=type, **kwargs)
    return Node("", storage, new_schema = schema_dict)

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




    # def initialize_node(self, schema:zarr_schema.ZarrNodeSchema):
    #     """ Write node to ZARR sotrage and create childs."""
    #     if schema is None:
    #         schema = {'VARS': {}, 'COORDS': {}, 'ATTRS': {}}
    #
    #     empty_ds = Node.empty_ds(schema)
    #     self.write_ds(empty_ds)
    #
    #     for key in schema.keys():
    #         if key in zarr_schema.reserved_keys:
    #             continue
    #         child = self._add_node(key)
    #         try:
    #             child.initialize_node(schema[key])
    #         except AttributeError:
    #             raise AttributeError(f"Expceting dict for key: {key}, got {schema[key]}")

    @staticmethod
    def _variable(var: zarr_schema.Variable, data: np.ndarray, coords_dict: Dict[str, Any]) -> xr.Variable:
        """
        Create an xarray Variable from `data` array and variable schema.
        All xarray variables should be created this way to ensure structure.
        """
        coord_names = set(coords_dict.keys())
        for coord in var.coords:
            assert coord in coord_names, f"Variable {var.name} has unknown coordinate {coord}."
        shape = tuple(0 for coord in var.coords)
        xr_var = xr.Variable(
                 dims=list(var.coords),  # Default dimension named after the coordinate key.
                 data=data,
                 attrs=var.attrs
             )
        return xr_var


    @staticmethod
    def _coord_variable(name: str, coord: zarr_schema.Coord, vars) -> xr.Variable:
        if coord.composed is None:
            composed = (name,)
        else:
            composed = coord.composed
        attrs = coord.attrs.copy()
        #attrs['composed'] = composed

        # simple coordinate
        np_var = vars[name]
        assert isinstance(np_var, np.ndarray)
        assert len(np_var.shape) == 1

        np_var = xr.Variable(
            dims=(name,),
            data=np_var,
            attrs=coord.attrs
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
            k: Node._coord_variable(k, c, vars)
            for k, c in coords.items()
        }
        return xr.Coordinates(coord_vars)

    @staticmethod
    def empty_ds(schema) -> xr.Dataset:
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
        attrs.update(schema['ATTRS'])
        ds = xr.Dataset(
            data_vars=variables,
            coords=coords_obj,
            attrs=attrs
        )
        return ds




    def __init__(self, name, store, parent=None, new_schema=None):
        """
        Parameters:
          name (str): The name of the node. For the root node, use an empty string ("").
          store (MutableMapping): The underlying Zarr store.
          parent (Node, optional): Parent node.
        """
        self.name = name
        self.store = store
        self.parent = parent

        # Setup logger
        zarr_root_dict = zarr.open_group(store).store
        self.logger = StoreLogHandler(zarr_root_dict)

        self.children = self._make_consistent(new_schema)
        # TODO: separate class to represent child nodes dict
        # it must be a separate class with overwritten setitem method to
        # maintain consistency between node tree and zarr storage.
        # Alternatively we can eliminate the dict like interface and only provide
        # access to keys and getitem method (minimalistic corresponding to the zarr storage API)


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

    def _add_node(self, name):
        assert name not in self.children
        node = Node(name, self.store, self)
        self.children[name] = node
        return node

    def _make_consistent(self, new_schema:zarr_schema.ZarrNodeSchema) -> 'Node':
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
        if new_schema is None:
            new_schema_ds = self.schema
            new_schema = defaultdict(lambda:None)
        else:
            new_schema_ds = {k: v for k, v in new_schema.items() if k in zarr_schema.reserved_keys}

        self._update_schema_ds(new_schema_ds)

        child_names = [key for key, _ in self._storage_group_paths()]
        child_names.extend([key for key in new_schema.keys() if key not in child_names and key not in zarr_schema.reserved_keys])

        def make_child(key):
            try:
                new_child_schema = new_schema[key]
            except KeyError:
                self.logger.warning(f"Key {key} is missing in the new schema, keeping it.")
                new_child_schema = None
            return Node(key, self.store, parent=self, new_schema=new_child_schema)

        # Process existing child Nodes
        childern = {
            key: make_child(key)
            for key in child_names
        }
        return childern

    def _update_schema_ds(self, new_schema):
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
        except KeyError:
            old_schema = {}
        if new_schema == old_schema:
            return old_schema
        assert old_schema == {}, (f"Modifying node dataset schema is not supported."
                                  f"\nold:{old_schema}\nnew{new_schema} ")
        empty_ds = Node.empty_ds(new_schema)
        self.write_ds(empty_ds)
        return new_schema


    def items(self):
        return self.children.items()

    def __getitem__(self, key):
        return self.children[key]

    # def _load_children(self):
    #     """
    #     Recursively find and attach child nodes from the store for this node.
    #     A child is detected if there exists a key of the form "{self.group_path}/{child}/.zgroup".
    #     """
    #     for key, group in self._storage_group_paths():
    #         child = self._add_node(key)
    #         child._load_children()

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
        ds = xr.open_zarr(self.store, group=rel_path)
        for coord in ds.coords:
            assert  'composed' in ds.coords[coord].attrs
        return ds

    @property
    def schema(self):
        """
        Return dictionary with node dataset schema.
        I.e. dictionary with keys 'COORDS', 'VARS', 'ATTRS'.
        Original schema tree is spread over the storage groups represented by Nodes.
        :return:
        """
        return zarr_schema.deserialize(self.dataset.attrs['__structure__'])



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
        ds = pivot_nd(self.schema, polars_df)
        written_ds, merged_coords = self.update_zarr_loop(ds)
        # check unique coords
        dup_dict = check_unique_coords(written_ds)
        if  dup_dict:
            self.logger.error(dup_dict)
        return written_ds

    def write_ds(self, ds, **kwargs):
        rel_path = self.group_path # + self.PATH_SEP + "dataset"
        rel_path = rel_path.strip(self.PATH_SEP)
        #path_store = zarr.open_group(self.store, mode=mode, path=rel_path)
        #ds.to_zarr(path_store,  **kwargs)
        ds.to_zarr(self.store, group = rel_path, mode="a", consolidated=False, **kwargs)
        return ds

    def update_zarr_loop(self, ds_update: xr.Dataset) -> xr.Dataset:
        """
        Iteratively update/append ds_update into an existing Zarr store at zarr_path.

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
        """
        ds_existing = self.dataset

        # --- Phase 1: Dive (split by dimension) ---
        # We create a dict to hold the extension subset for each dimension.
        if '__empty__' in ds_existing.attrs:
            return self.write_ds(ds_update), {}

        last_written_ds = None
        ds_extend_dict = {}
        ds_overlap = ds_update.copy()
        dims_order = tuple(ds_update.dims.keys())
        for dim in dims_order:
            if dim not in ds_existing.dims:
                raise ValueError(f"Dimension '{dim}' not found in the existing store.")
            old_coords = ds_existing[dim].values
            new_coords = ds_overlap[dim].values
            min_new, max_new = np.min(new_coords), np.max(new_coords)
            new_holes = [c for c in old_coords if c > min_new or c < max_new]
            print(f"Update Zarr: {dim}, new_holes: {new_holes}")
            min_old, max_old = np.min(old_coords), np.max(old_coords)
            old_holes = [c for c in new_coords if c > min_old or c < max_old]
            print(f"Update Zarr: {dim}, old_holes: {old_holes}")

            # Determine which coordinates in ds_current already exist.
            overlap_mask = np.isin(new_coords, old_coords)
            ds_extend_dict[dim] = ds_overlap.sel({dim: new_coords[~overlap_mask]})
            ds_overlap = ds_overlap.sel({dim: new_coords[overlap_mask]})

        # At this point, ds_overlap covers only the coordinates that already exist in the store
        # in every dimension in dims_order. Write these (overlapping) data using region="auto".
        update_overlap_size = np.prod(list(ds_overlap.sizes.values()))
        if update_overlap_size > 0:
            last_written_ds = self.write_ds(ds_overlap, region="auto")

        # --- Phase 2: Upward (process extension subsets in reverse order) ---
        # We also update a merged_coords dict from the store.
        merged_coords = {d: ds_existing[d].values for d in ds_existing.dims}

        # Loop upward in reverse order over dims_order.
        for dim in reversed(dims_order):
            dim_coord = ds_extend_dict[dim]
            if dim_coord is None or dim_coord.sizes.get(dim, 0) == 0:
                continue  # No new coordinates along this dimension.

            # For all dimensions other than dim, reindex ds_ext so that the coordinate arrays
            # come from the store (i.e. the full arrays). This ensures consistency.
            # (This constructs an indexers dict using the existing merged coordinates.)
            indexers = {d: merged_coords[d] for d in dim_coord.dims if d != dim}
            ds_ext_reindexed = dim_coord.reindex(indexers, fill_value=np.nan)

            # Append the extension subset along the current dimension.
            last_written_ds = self.write_ds(ds_ext_reindexed, append_dim=dim)

            # Update merged coordinate for dim: concatenate the old coords with the new ones.
            new_coords_for_dim = dim_coord[dim].values
            merged_coords[dim] = np.concatenate([merged_coords[dim], new_coords_for_dim])

        assert last_written_ds is not None, "No data was written to the dataset."
        return last_written_ds, merged_coords


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
                raise ValueError(f"Values along axis {axis} are not all equal")
            # Remove this axis by selecting the 0th element along that axis.
            arr = np.take(arr, 0, axis=axis)
    return arr

def get_df_col(df, col_name):
    try:
        return df[col_name].to_numpy()
    except pl.exceptions.ColumnNotFoundError:
        return np.full(df.shape[0], np.nan)
        # TODO: log missing column
        #raise ValueError(f"Column '{col_name}' not found in the DataFrame.\n Valid columns: {df.columns}")


def pivot_nd(structure:zarr_schema.ZarrNodeSchema, df: pl.DataFrame, fill_value=np.nan):
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
    """
    # 1. DF -> dict of 1d arrays,
    data_vars = {
        k: get_df_col(df, var.df_col)
        for k, var in structure['VARS'].items()
    }
    # 2. apply hash of tuple coords, input Dict: ds_name : [df_cols]
    for k, c in structure['COORDS'].items():
        if (c.composed is not None) and (len(c.composed) > 1):
            # hash tuple coords
            tuple_list = zip( *(data_vars[c] for c in c.composed) )
            hash_list = [hash(tuple(t)) for t in tuple_list]
            data_vars[k] = np.array(hash_list)

    # 3. Extract coords
    idx_list = []
    coords_dict = {}
    dims = list(structure['COORDS'].keys())
    # Loop over each dimension in the original dataset.
    for d in dims:
        # Get the name(s) of the column(s) in df corresponding to this dimension.
        df_coord_array = data_vars[d]
        # Get the coordinate values from the dataset (assumed to be in desired order).
        coords = np.sort(np.unique(df_coord_array))
        coords_dict[d] = coords  # will be used as the coordinate values for this dim.
        #coord_sizes[d].append(len(coords))
        # Map each row’s coordinate (from df) to its index in the common_coords.
        # (This works as long as common_coords is sorted. In many cases ds coordinates are already sorted.)
        final_idx = np.searchsorted(coords, df_coord_array)
        idx_list.append(final_idx)
    coord_sizes = [len(coords_dict[d]) for d in dims]
    # Create an array of indices for all dimensions, one row per dimension.
    idx_arr = np.vstack(idx_list)
    # Compute flat indices for the N-dim output array.
    flat_idx = np.ravel_multi_index(idx_arr, dims=coord_sizes)
    coords_dict = Node._create_coords(structure['COORDS'], coords_dict)

    # 4. form variables
    # Helper: for a given variable, build the pivoted array.
    def form_var_array(var_struc):
        column_vals = data_vars[var_struc.name]
        # Choose a dtype based on the column (floating or object)
        dtype = column_vals.dtype if np.issubdtype(column_vals.dtype, np.floating) else object
        # Create an empty output array with fill_value.
        result = np.full(coord_sizes, fill_value, dtype=dtype)
        # Fill in the values; if duplicate keys occur, later values win.
        result.flat[flat_idx] = column_vals
        # eliminate inappropriate coordinates
        dims_to_eliminate = [d not in var_struc.coords for d in dims]
        np_var = eliminate_dims_if_equal(result, dims_to_eliminate)
        data_var = Node._variable(var_struc, np_var, coords_dict)
        return data_var

    # 4. form arrays (remaining vars)
    # Create DataArrays for each variable using the dims specified in self.dataset.
    data_vars = {k: form_var_array(var)
                 for k, var in structure['VARS'].items()
                 if k not in coords_dict}

    # Build and return the xarray.Dataset with the computed coordinates.
    attrs = structure['ATTRS']
    attrs['__structure__'] = zarr_schema.serialize(structure)
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