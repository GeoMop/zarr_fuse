import json
import yaml
import numpy as np
import numpy.testing as npt
from pathlib import Path
import polars as pl
import xarray as xr
import pytest
import zarr

from zarr_fuse  import Node, schema


script_dir = Path(__file__).parent
inputs_dir = script_dir / "inputs"
workdir = script_dir / "workdir"




@pytest.mark.skip
def test_update_xarray_nd():
    # Create an initial dataset with dimensions: time, loc, sensor.
    # For fixed dims "loc" and "sensor", we allocate extra space.
    ds = xr.Dataset(
        data_vars={"var_1": (("time", "loc", "sensor"),
                           np.full((2, 3, 2), 100.0, dtype=float)),
                   "var_2": (("time", "loc", "sensor"),
                    np.full((2, 3, 2), 100.0, dtype=float))
                   },
        coords={
            "time": np.array([1, 2]),
            "loc": np.array(["C", "A", "_B"]),  # allocated size=3, initially valid size=2
            "sensor": np.array(["Y", "X"])  # allocated size=2, valid size=2
        }
    )
    ds.attrs["append_dim"] = "time"
    ds.attrs["coords_valid_size"] = {"time": 2, "loc": 2, "sensor": 2}

    # Create an update DataFrame.
    # For time=2, update: (loc="A", sensor="X") -> 200.0 and (loc="D", sensor="X") -> 210.0.
    # Note: "D" is a new fixed value for "loc" (should be appended to valid region if space permits).
    # For time=3 (new dynamic), update: (loc="B", sensor="Y") -> 300.0.
    df = pl.DataFrame({
        "time": [2, 2, 2,  3, 3, 3,  3, 3, 3],
        "loc": ["C", "A", "B", "C", "A", "B", "C", "A", "B"],
        "sensor": ["X", "X", "X", "X", "X", "X", "Y", "Y", "Y"],
        "var_1": [200.0, 210.0, 220.0,   300.0, 310.0, 320.0,   350.0, 360.0, 370.0],
        "var_2": np.array([200.0, 210.0, 220.0,   300.0, 310.0, 320.0,   350.0, 360.0, 370.0])+1000,
    })

    ds_updated = update_xarray_nd(ds, df)

    # Check that the dynamic dimension "time" now includes time=3.
    updated_times = ds_updated.coords["time"].values
    np.testing.assert_array_equal(updated_times, np.array([1, 2, 3]))

    # Check that fixed dimension "loc" now has an increased valid region including "D".
    assert ds_updated.attrs["coords_valid_size"]["loc"] == 3
    loc_valid = ds_updated.coords["loc"].values
    np.testing.assert_array_equal(loc_valid, np.array(["C", "A", "B"]))

    # Verify variable updates:
    # At time=2, (loc="A", sensor="X") should be updated to 200.0.
    var_1 = ds_updated["var_1"].to_numpy()
    ref_var_1 = np.array([
        # time = 1 (unchanged)
        [[100.0, 100.0],  # loc "A": sensor "Y", "X"
         [100.0, 100.0],  # loc "B"
         [100.0, 100.0]],  # loc "C"

        # time = 2
        [[100.0, 200.0],  # loc "A": sensor "Y" remains 100, sensor "X" updated to 210.0
         [100.0, 210.0],  # loc "B": sensor "Y" remains 100, sensor "X" updated to 220.0
         [100.0, 220.0]],  # loc "C": sensor "Y" remains 100, sensor "X" updated to 200.0

        # time = 3
        [[350.0, 300.0],  # loc "A": sensor "Y" updated to 360, sensor "X" updated to 310
         [360.0, 310.0],  # loc "B": sensor "Y" updated to 370, sensor "X" updated to 320
         [370.0, 320.0]]  # loc "C": sensor "Y" updated to 350, sensor "X" updated to 300
    ])

    np.testing.assert_almost_equal(var_1, ref_var_1)

    var_2 = ds_updated["var_2"].to_numpy()
    ref_var_2 = np.array([
        # time = 1 (unchanged)
        [[100.0, 100.0],
         [100.0, 100.0],
         [100.0, 100.0]],

        # time = 2
        [[100.0, 1200.0],  # loc "A": sensor "Y" remains 100, sensor "X" becomes 1210.0
         [100.0, 1210.0],  # loc "B": sensor "Y" remains 100, sensor "X" becomes 1220.0
         [100.0, 1220.0]],  # loc "C": sensor "Y" remains 100, sensor "X" becomes 1200.0

        # time = 3
        [[1350.0, 1300.0],  # loc "A": sensor "Y" becomes 1360, sensor "X" becomes 1310
         [1360.0, 1310.0],  # loc "B": sensor "Y" becomes 1370, sensor "X" becomes 1320
         [1370.0, 1320.0]]  # loc "C": sensor "Y" becomes 1350, sensor "X" becomes 1300
    ])
    np.testing.assert_almost_equal(var_2, ref_var_2)

    print("Test passed: update_xarray_nd with separate coordinate processing works correctly.")

"""
This is an inital test of xarray, zarr functionality that we build on.
This requires dask.
"""
def aux_read_struc(fname):
    struc_path = inputs_dir / fname
    structure = schema.deserialize(struc_path)
    assert set(['COORDS', 'VARS']).issubset(set(structure.keys()))

    store_path = (workdir / fname).with_suffix(".zarr")
    local_store = zarr.storage.LocalStore(store_path)

    # memory_store = zarr.storage.MemoryStore()

    # zip_store = zarr.storage.ZipStore('path/to/archive.zip', mode='w')

    # s3_fs = fsspec.filesystem('s3', key='YOUR_ACCESS_KEY', secret='YOUR_SECRET_KEY')
    # s3_store = zarr.FSStore('bucket-name/path/to/zarr', filesystem=s3_fs)

    tree = Node.create_storage(structure, local_store)
    return structure, local_store, tree


# Recursively update each node with its corresponding data.
def _update_tree(node: Node, df_map: dict):
    if node.group_path in df_map:
        node.update(df_map[node.group_path])
    for key, child in node.items():
        _update_tree(child, df_map)

    assert len(node.dataset.coords) == 1
    assert len(node.dataset.data_vars) == 1


def test_node_tree():
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

    root_node = Node.read_store(store)
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



def test_read_structure_weather(tmp_path):
    # Example YAML file content (as a string for illustration):
    structure, store, tree = aux_read_struc("structure_weather.yaml")
    assert len(structure['COORDS']) == 2
    assert len(structure['VARS']) == 4
    print("Coordinates:")
    for coord in structure["COORDS"]:
        print(coord)
    print("\nQuantities:")
    for var in structure["VARS"]:
        print(var)

    # Create a Polars DataFrame with 6 temperature readings.
    # Two time stamps (e.g. 1000 and 2000 seconds) and three latitude values (e.g. 10.0, 20.0, 30.0).
    df = pl.DataFrame({
        "timestamp": [1000, 1000, 1000, 2000, 2000, 2000],
        "latitude": [10.0, 20.0, 20.0, 10.0, 20.0, 20.0],
        "longitude": [10.0, 10.0, 20.0, 10.0, 10.0, 20.0],
        "temp": [280.0, 281.0, 282.0, 283.0, 284.0, 285.0]
    })

    # Update the dataset atomically using the Polars DataFrame.
    updated_ds = tree.update(df)

    # Now, re-read the entire Zarr storage from scratch.
    new_tree = Node.read_store(store)
    new_ds = new_tree.dataset
    print("Updated dataset:")
    print(new_ds)

    # --- Assertions ---
    # We expect that the update function (via update_xarray_nd) will reshape the temperature data
    # into a (time, lat) array, i.e. shape (2, 3), with coordinates "time" and "lat".
    # Check the shape of the temperature variable.
    assert new_ds["temperature"].shape == (2, 3)

    # Check that the "time" coordinate was updated to [1000, 2000]
    np.testing.assert_array_equal(new_ds["time of year"].values, [1000, 2000])
    # Check that the "lat" coordinate was updated to [10.0, 20.0, 30.0]
    np.testing.assert_array_equal(new_ds["latitude"].values, [20.0, 20.0, 10.0])
    for row in df.iter_rows(named=True):
        time = row["timestamp"]
        lat = row["latitude"]
        lon = row["longitude"]
        assert new_ds["temperature"].sel({"time of year":time, "lat_lon":hash((lat, lon))}) == row["temp"]


def test_read_structure_tensors(tmp_path):
    structure, store, tree = aux_read_struc("structure_tensors.yaml")
    assert len(structure['COORDS']) == 3
    assert len(structure['VARS']) == 5
    print("Coordinates:")
    for coord in structure["COORDS"]:
        print(coord)
    print("\nQuantities:")
    for var in structure["VARS"]:
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

    # Write the dataset to the Zarr store at the root group.
    ds.to_zarr(store, mode="w")

    # Create a Node instance for the root (empty name) with the given store.
    node = Node("", store)

    # Use read_df to select a subset of the data.
    # For example, select times from "2025-01-02" to "2025-01-04" (inclusive).
    df_polars = node.read_df("temperature", time=slice("2025-01-02", "2025-01-04"))

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



