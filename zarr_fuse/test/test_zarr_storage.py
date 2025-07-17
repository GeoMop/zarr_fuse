import shutil
import time

import numpy as np
import numpy.testing as npt
from pathlib import Path
import polars as pl
import xarray as xr
import pytest
import zarr
import time
import asyncio, s3fs
import fsspec
from zarr.storage import FsspecStore

import zarr_fuse as zf


script_dir = Path(__file__).parent
inputs_dir = script_dir / "inputs"
workdir = script_dir / "workdir"

import asyncio

def sync_remove_store(storage_options, path):
    so = storage_options.copy()
    so['asynchronous'] = False  # Ensure synchronous operation
    fs = fsspec.filesystem('s3', **so)
    try:
        fs.rm(path, recursive=True, maxdepth=None)
    except FileNotFoundError:
        pass

def sync_list_dirs(storage_options, root_path):
    so = storage_options.copy()
    so['asynchronous'] = False
    fs = fsspec.filesystem('s3', **so)
    directories = []

    def _list_dirs(path):
        try:
            entries = fs.ls(path, detail=True)
        except FileNotFoundError:
            return
        for entry in entries:
            # For s3fs, entry['type'] is 'directory' for dirs, 'file' for files
            if entry.get('type') == 'directory':
                directories.append(entry['name'])
                _list_dirs(entry['name'])

    _list_dirs(root_path)
    return directories


"""
This is an inital test of xarray, zarr functionality that we build on.
This requires dask.
"""
def aux_read_struc(fname):
    """
    Read a schema file from 'inputs_dir',
    :param fname:
    :return:
    """
    struc_path = inputs_dir / fname
    schema = zf.schema.deserialize(struc_path)
    store_path = (workdir / fname).with_suffix(".zarr")

    # Start with no existiong storage
    # if store_path.exists():
    #     shutil.rmtree(store_path)
    # local_store = zarr.storage.LocalStore(store_path)
    # tree = zf.Node("", local_store, new_schema=schema)

    #    memory_store = zarr.storage.MemoryStore()

    bucket_name = "test-zarr-storage"
    s3_key = "4UD5K2LCS5ZU8GHL5TJS"
    s3_secret = "VztZ2COyVsgADEGbftd1Zt6XdtN6QXwOhSfEKT0Y"
    storage_options = dict(
        key=s3_key,
        secret=s3_secret,
        # asynchronous=True,
        listings_expiry_time=1,  # Expire listing cache after 1 second
        max_paths=0,  # Do not cache directory listings
        client_kwargs=dict(
            endpoint_url="https://s3.cl4.du.cesnet.cz"),
        config_kwargs={
            "s3": {"addressing_style": "path"},
            "retries": {"max_attempts": 5, "mode": "standard"},
            "request_checksum_calculation": "when_required",
            "response_checksum_validation": "when_required",
        },
    )
    root_path = f"{bucket_name}/test.zarr"
    sync_remove_store(storage_options, root_path)
    fs = fsspec.filesystem('s3', **storage_options)

    store = zarr.storage.FsspecStore(fs, path=root_path)

    # TODO: debug, whot happens to my loop
    # - self containt example - > FsSpec issue

    #s3_store.fs.put(str(store_path), root_path, recursive=True)
    #fs.invalidate_cache(root_path)
    print("Schema file stored.")
    return schema, store, zf.Node("", store, new_schema=schema) # You may need to re-instantiate the Node with the S3 store

# Recursively update each node with its corresponding data.
def _update_tree(node: zf.Node, df_map: dict):
    if node.group_path in df_map:
        #print(f"Updating node {node.group_path}.")
        #assert (Path(node.store.root) / node.group_path).exists()
        node.update(df_map[node.group_path])
    for key, child in node.items():
        _update_tree(child, df_map)

    assert len(node.dataset.coords) == 1
    assert len(node.dataset.data_vars) == 1

def test_node_tree():
    """
    Test the tree structure of the Zarr storage.
    :return:
    """
    # Read the YAML file from the working directory.
    # The file "structure_tree.yaml" must exist in the current working directory.
    # Example YAML file content (as a string for illustration):
    structure, store, tree = aux_read_struc("structure_tree.yaml")

    # Create a mapping from node names to minimal Polars DataFrames.
    # Each node is updated with unique values.
    df_map = {
        "": pl.DataFrame({"time": [1000], "temperature": [280.0]}),
        "child_1": pl.DataFrame({"time": [1001], "temperature": [281.0]}),
        "child_2": pl.DataFrame({"time": [1002], "temperature": [282.0]}),
        "child_1/child_3": pl.DataFrame({"time": [1003], "temperature": [283.0]}),
    }

    _update_tree(tree, df_map)

    # Recursively collect nodes into a dictionary for easy lookup.
    def collect_nodes(node, nodes_dict):
        nodes_dict[node.group_path] = node
        for key, child in node.items():
            collect_nodes(child, nodes_dict)
        return nodes_dict

    zarr.consolidate_metadata(store)
    root_node = zf.Node.read_store(store)
    nodes = collect_nodes(root_node, {})

    # Expected values for each node: (time coordinate, temperature variable)
    expected = {
        key: (df['time'].to_numpy(), df['temperature'].to_numpy())
        for key, df in df_map.items()
    }

    # Verify that each node’s dataset contains the expected coordinate and variable data.
    for node_name, (exp_time, exp_temp) in expected.items():
        ds = nodes[node_name].dataset
        np.testing.assert_array_equal(ds.coords["time"].values, exp_time)
        np.testing.assert_array_equal(ds["temperature"].values, exp_temp)

    # Verify the tree structure:
    # The root node should have children "child_1" and "child_2"
    assert set(root_node.children.keys()) == {"child_1", "child_2"}
    # Node "child_1" should have one child: "child_3"
    assert set(root_node.children["child_1"].children.keys()) == {"child_3"}


def _check_ds_attrs_weather(ds, schema_ds):
    # Check that the dataset has the expected attributes.
    assert "description" in ds.attrs
    assert "__structure__" in ds.attrs
    for key, coord in schema_ds.COORDS.items():
        assert key in ds.coords
        assert ds.coords[key].attrs['composed'] == coord.composed
        assert ds.coords[key].attrs['chunk_size'] == coord.chunk_size
        if len(coord.composed) > 1:
            assert ds.coords[key].dtype == 'int64'
            for sub_coord in coord.composed:
                assert sub_coord in ds.data_vars
                assert sub_coord not in ds.coords

def test_read_structure_weather(tmp_path):
    # Example YAML file content (as a string for illustration):
    structure, store, tree = aux_read_struc("structure_weather.yaml")
    ds_schema = structure.ds
    assert len(ds_schema.COORDS) == 2
    assert len(ds_schema.VARS) == 4
    print("Coordinates:")
    for coord in ds_schema.COORDS:
        print(coord)
    print("\nQuantities:")
    for var in ds_schema.VARS:
        print(var)

    # Create a Polars DataFrame with 6 temperature readings.
    # Two time stamps (e.g. 1000 and 2000 seconds) and three latitude values (e.g. 10.0, 20.0, 30.0).
    t1 = "2025-05-13T07:00:00Z"
    t2 = "2025-05-13T09:00:00Z"
    t3 = "2025-05-13T8:00:00Z"
    t4 = "2025-05-14T8:00:00Z"

    df = pl.DataFrame({
        "timestamp": [t1, t1, t1, t2, t2, t2],
        "latitude": [10.0, 20.0, 20.0, 10.0, 20.0, 20.0],
        "longitude": [10.0, 10.0, 20.0, 10.0, 10.0, 20.0],
        "temp": [280.0, 281.0, 282.0, 283.0, 284.0, 285.0]
    })

    # Update the dataset atomically using the Polars DataFrame.
    updated_ds = tree.update(df)
    _check_ds_attrs_weather(updated_ds, ds_schema)

    # Now, re-read the entire Zarr storage from scratch.
    new_tree = zf.Node.read_store(store)
    new_ds = new_tree.dataset
    _check_ds_attrs_weather(new_ds, ds_schema)
    print("Updated dataset:")
    print(new_ds)

    # --- Assertions ---
    # We expect that the update function (via update_xarray_nd) will reshape the temperature data
    # into a (time, lat) array, i.e. shape (2, 3), with coordinates "time" and "lat".
    # Check the shape of the temperature variable.
    assert new_ds["temperature"].shape == (2, 3)

    # Check that the "time" coordinate, it is converted from explicit UTC ("...Z") to CET
    # during forming the update DF and the converted back to UTC during actual update.
    ref_vec = np.array([t1, t2], dtype='datetime64[h]')
    np.testing.assert_array_equal(new_ds["time of year"].values, ref_vec)

    # Check that the "lat" coordinate was updated to [10.0, 20.0, 30.0]
    np.testing.assert_array_equal(new_ds["latitude"].values, [20.0, 20.0, 10.0])
    out_unit = zf.units.DateTimeUnit(tick='h', tz="UTC", dayfirst=False, yearfirst=True)
    for row in df.iter_rows(named=True):

        time = zf.units.create_quantity([row["timestamp"]], out_unit).magnitude
        lat = row["latitude"]
        lon = row["longitude"]
        new_temp = new_ds["temperature"].sel({"time of year":time, "lat_lon":hash((lat, lon))})
        ref_temp_K = row["temp"] + 273.15
        assert  new_temp.values[0] == ref_temp_K

    # Second update, test merging
    df2 = pl.DataFrame({
        "timestamp": [t4, t4, t4, t3, t3, t3],  #  t1 < t3 < t2 < t4
        "latitude": [20.0, 10.0, 20.0, 10.0, 20.0, 20.0],
        "longitude": [10.0, 10.0, 20.0, 10.0, 20.0, 10.0],
        "temp": [381.0, 380.0, 382.0, 383.0, 385.0, 384.0]
    })

    # Update the dataset atomically using the Polars DataFrame.
    updated_ds = tree.update(df2)
    # Time t3 is only used to interpolate to t2, not added to the dataset.
    assert [*updated_ds.sizes.values()] == [1, 3]
    _check_ds_attrs_weather(updated_ds, ds_schema)

    # Now, re-read the entire Zarr storage from scratch.
    new_tree = zf.Node.read_store(store)
    new_ds = new_tree.dataset
    _check_ds_attrs_weather(new_ds, ds_schema)
    print("Updated dataset:")
    print(new_ds)

   # --- Assertions ---
    # We expect that the update function (via update_xarray_nd) will reshape the temperature data
    # into a (time, lat) array, i.e. shape (2, 3), with coordinates "time" and "lat".
    # Check the shape of the temperature variable.
    assert new_ds["temperature"].shape == (3, 3)

    # Check that the "time" coordinate was updated to [1000, 2000]

    # check times are sorted
    import pandas as pd

    times_pd = pd.to_datetime([t1, t2, t4], utc=True)
    ref_times = times_pd.values.astype("datetime64[ns]")
    np.testing.assert_array_equal(new_ds["time of year"].values, ref_times)
    # !! Wrong order, not sorted


    # Check that the "lat" coordinate was updated to [10.0, 20.0, 30.0]
    np.testing.assert_array_equal(new_ds["latitude"].values, [20.0, 20.0, 10.0])
    np.testing.assert_array_equal(new_ds["longitude"].values, [20.0, 10.0, 10.0])

    # TODO, merged DF, test NaNs out of the update.
    # merged_df = df.update(df2)
    # for row in df.iter_rows(named=True):
    #     time = row["timestamp"]
    #     lat = row["latitude"]
    #     lon = row["longitude"]
    #     assert new_ds["temperature"].sel({"time of year":time, "lat_lon":hash((lat, lon))}) == row["temp"]


def test_read_structure_tensors(tmp_path):
    structure, store, tree = aux_read_struc("structure_tensors.yaml")
    ds_schema = structure.ds
    assert len(ds_schema.COORDS) == 3
    assert len(ds_schema.VARS) == 5
    print("Coordinates:")
    for coord in ds_schema.COORDS:
        print(coord)
    print("\nQuantities:")
    for var in ds_schema.VARS:
        print(var)



def test_node_read_df():
    # Create an in‑memory Zarr store (a dict works as a Zarr store)
    store = {}

    # Create a simple dataset with one coordinate ("time") and one variable ("temperature").
    # We use 5 time points with increasing temperature values.
    times = np.array(
        ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"],
        dtype="datetime64[ns]"
    )
    temperature = xr.DataArray(
        np.arange(5),
        dims=["time"],
        coords={"time": times}
    )
    ds = xr.Dataset({"temperature": temperature})
    ds.coords["time"].attrs["composed"] = ["time"]

    # Write the dataset to the Zarr store at the root group.
    #ds.to_zarr(store, mode="w")

    # Create a Node instance for the root (empty name) with the given store.
    #node = zf.Node("", store)

    # Use read_df to select a subset of the data.
    # For example, select times from "2025-01-02" to "2025-01-04" (inclusive).
    df_polars = zf.Node._read_df(ds, "temperature", time=slice("2025-01-02", "2025-01-04"))

    # Convert the Polars DataFrame to Pandas for easier assertions.
    df = df_polars.to_pandas()

    # Expected results:
    # The subset should contain rows corresponding to "2025-01-02", "2025-01-03", "2025-01-04"
    expected_times = np.array(
        ["2025-01-02", "2025-01-03", "2025-01-04"],
        dtype="datetime64[ns]"
    )
    expected_temp = np.array([1, 2, 3])

    # Verify the number of rows is as expected.
    assert df.shape[0] == 3, "Expected three rows in the subset."

    # Check that the 'time' column matches the expected times.
    np.testing.assert_array_equal(df["time"].values, expected_times)

    # Check that the 'temperature' values match.
    np.testing.assert_array_equal(df["temperature"].values, expected_temp)





def update_zarr_store(zarr_path: str, ds_update: xr.Dataset) -> None:
    """
    Update/append 'ds_update' into an existing Zarr store at 'zarr_path'.
    - Dims: 'time' and 'x'.
    - Possibly extends the 'time' dimension if new time coords go beyond existing range.
    - Uses region='auto' so xarray infers which slices to overwrite/append.
    """
    ds_update.to_zarr(
        zarr_path,
        mode="r+",        # read/write (store must already exist)
        region="auto"     # infer slice(s) from coords to overwrite or append
    )




@pytest.fixture
def sample_df():
    # Create a sample Polars DataFrame with multi-index columns: time_stamp and location,
    # plus two data columns: var1 and var2.
    location = [(1.,2.), (3., 4.0), (5., 6.0), (1., 3.)]
    loc_x, loc_y = zip(*location)
    loc_struct = pl.DataFrame(dict(x=loc_x, y=loc_y)).to_struct()
    df = pl.DataFrame({
        "time_stamp": [1, 2, 3, 4],
        "location": loc_struct,
        "sensor": ["A", "B", "C", "A"],
        "var1": [0.0, 0.0, 0.0, 0.0],
        "var2": [0, 0, 0, 0]
    })
    return df


@pytest.mark.skip
def test_pivot_nd():
    # Create a sample Polars DataFrame with columns for a 3D multi-index.
    df = pl.DataFrame({
        "time": [1, 1, 1, 2, 2],
        "loc": ["A", "A", "B", "A", "B"],
        "sensor": ["X", "Y", "X", "X", "Y"],
        "var": [10.0, 11.0, 12.0, 20.0, 21.0]
    })
    dims = ["time", "loc", "sensor"]

    # Call pivot_nd to generate the N-d array and the coordinate mapping.
    arr, unique_coords = pivot_nd(df, dims, "var", fill_value=np.nan)

    # Expected unique coordinates:
    # time: [1, 2]
    # loc:  ["A", "B"]
    # sensor: ["X", "Y"]
    expected_coords = {
        "time": np.array([1, 2]),
        "loc": np.array(["A", "B"]),
        "sensor": np.array(["X", "Y"])
    }

    # Check that unique_coords match.
    for d in dims:
        np.testing.assert_array_equal(unique_coords[d], expected_coords[d])

    # The resulting array should have shape (2, 2, 2)
    assert arr.shape == (2, 2, 2)

    # Expected array construction:
    # For time=1, loc="A", sensor="X": 10.0
    # For time=1, loc="A", sensor="Y": 11.0
    # For time=1, loc="B", sensor="X": 12.0
    # For time=1, loc="B", sensor="Y": missing -> NaN
    # For time=2, loc="A", sensor="X": 20.0
    # For time=2, loc="A", sensor="Y": missing -> NaN
    # For time=2, loc="B", sensor="X": missing -> NaN
    # For time=2, loc="B", sensor="Y": 21.0
    expected_arr = np.array([
        [[10.0, 11.0],
         [12.0, np.nan]],
        [[20.0, np.nan],
         [np.nan, 21.0]]
    ])

    np.testing.assert_allclose(arr, expected_arr, equal_nan=True)
