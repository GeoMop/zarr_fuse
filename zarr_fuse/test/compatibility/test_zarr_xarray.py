import numpy as np
import xarray as xr
from pathlib import Path
import shutil
import zarr
script_dir = Path(__file__).parent
workdir = script_dir / "workdir"



def make_ds(coords_, ds=None):
    def make_coord(name, coord):
        if isinstance(coord, np.ndarray):
            return coord
        elif isinstance(coord, int):
            return np.arange(coord)
        elif coord is None:
            return ds[name].values
        else:
            raise ValueError(f"Invalid coordinate for {name}: {coord}")
    coords = {
        d: make_coord(d, coord)
        for d, coord in coords_.items()
    }
    shape = tuple(len(c) for c in coords.values())
    data = np.random.rand(*shape)
    return xr.Dataset(
        {"temp": (tuple(coords.keys()), data)},
        coords=coords
    )

consolidated = True

def group_initialize(store, group):
    attrs = {"__empty__": True, "description": "just an init placeholder"}

    # open (or create) the root Zarr group in “write” mode
    grp = zarr.open_group(store, path=group, mode="w")
    # set your dataset‐level attributes
    grp.attrs.update(attrs)


def group_raw_zarr_operations(store, group):

    def write_ds(ds, **kwargs):
        """
        Write dataset to Zarr store with given kwargs.
        """
        ds.to_zarr(store, group=group, consolidated=consolidated, **kwargs)

    def read_ds():
        return xr.open_zarr(store, group=group, consolidated=consolidated)

    assert (store / "zarr.json").exists()


    # 1) Create and write an initial dataset
    ds = make_ds(dict(
        time=np.array(["2020-01-01","2020-02-01"], dtype="datetime64[ns]"),
        x = 3,
        y = 4))
    write_ds(ds, mode="a")

    ds_update = make_ds({
        "time": ds['time'].values,
        "x": ds['x'].values[2:],
        "y": ds['y'].values[1:]})
    # 3) Overwrite that contiguous slice in-place
    write_ds(ds_update, mode="r+", region="auto")

    # 4) Re-open and verify
    ds_full = read_ds()
    #expected = np.array([0, 10, 11, 12, 4])
    np.testing.assert_array_equal(
        ds_full["temp"].values[:, 2:, 1:],
        ds_update["temp"].values,
        err_msg="Only the middle slice should have been overwritten"
    )

    # 2) Append along 'time'
    times1 = np.array(["2020-03-01"], dtype="datetime64[ns]")
    ds1 = make_ds({"time": times1, "x": None, "y": None}, ds=read_ds())
    write_ds(ds1, mode="a", append_dim="time")

    # Now store has time=3, x=3, y=4
    assert(read_ds().sizes == {'time': 3, 'x': 3, 'y': 4})


    # 3) Append along 'x'
    #   – You must supply exactly the same 'time' & 'y' coords
    #   – and a new chunk of length 1 along 'x':

    ds2 = make_ds({"time":None, "x":np.array([3]), "y":None}, ds=read_ds())
    write_ds(ds2, mode="a", append_dim="x")

    # Final sizes
    assert(read_ds().sizes == {'time': 3, 'x': 4, 'y': 4})
    # → {'time': 3, 'x': 4, 'y': 4}

def test_raw_zarr_operations():
    """
    Test raw Zarr operations on a group.
    """

    store = workdir / "mydata.zarr"
    shutil.rmtree(store, ignore_errors=True)

    group_initialize(store, "g1")
    group_initialize(store, "g1/g2")
    group_raw_zarr_operations(store, "g1")
    assert (store / "g1" / "zarr.json").exists()
    assert (store / "g1" / "g2"/"zarr.json").exists()

    group_raw_zarr_operations(store, "g1/g2")
    assert (store / "g1" / "zarr.json").exists()
    assert (store / "g1" / "g2"/"zarr.json").exists()

    ds_g1 = xr.open_zarr(store, group="g1")
    ds_g2 = xr.open_zarr(store, group="g1/g2")
    assert ds_g1.sizes == ds_g2.sizes
    assert np.any(ds_g1["temp"].values != ds_g2["temp"].values)
