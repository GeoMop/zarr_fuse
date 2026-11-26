"""Exercise the window reader against a local Zarr store with real chunks."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from zarr.storage import LocalStore

from dashboard.config import SchemaConfig, SchemaFieldsConfig
from dashboard.data import LocalClient
from dashboard.scripts.benchmark_timeseries import CountingStore


@pytest.fixture(params=[{}, None], ids=["storage-chunks", "backend-indexing"])
def source(tmp_path, monkeypatch, request):
    """Write measurements in two dimension orders, with gaps and repeated times."""
    times = pd.Timestamp("2024-01-01") + pd.to_timedelta(
        [0, 2, 9, 9, 25, 31, 59, 65], unit="min"
    )
    values = np.arange(48, dtype=np.float32).reshape(8, 2, 3)
    values[2:4, 1, 2] = np.nan
    ds = xr.Dataset(
        {
            "temp": (("time", "site", "depth"), values),
            "reordered": (("depth", "time", "site"), values.transpose(2, 0, 1)),
            "air": (("time", "site"), values[:, :, 0]),
            "unsorted": ("other_time", times[[1, 0, 2, 3, 4, 5, 6, 7]]),
        },
        coords={"time": times, "site": ["A", "B"], "depth": [5.0, 10.0, 20.0]},
    )
    path = tmp_path / "fixture.zarr"
    ds.to_zarr(
        path,
        mode="w",
        consolidated=False,
        zarr_format=3,
        group="sensors",
        encoding={
            "temp": {"chunks": (3, 1, 2)},
            "reordered": {"chunks": (2, 3, 1)},
            "air": {"chunks": (3, 1)},
        },
    )
    schema = SchemaConfig(
        file="", fields=SchemaFieldsConfig("lat", "lon", "time", "site", "depth")
    )
    monkeypatch.setattr(
        LocalClient, "_view_config", lambda self, view: SimpleNamespace(schema=schema)
    )
    store = CountingStore(LocalStore(path, read_only=True))
    opens = []

    class Group:
        @property
        def dataset(self):
            opens.append("open")
            return xr.open_zarr(
                store, group="sensors", consolidated=False, chunks=request.param
            )

    monkeypatch.setattr(LocalClient, "_get_group", lambda self, view, group: Group())
    with store:
        yield LocalClient(Path("unused.yaml")), ds, store, opens


def _request(client, **kwargs):
    args = {
        "variable": "temp",
        "entity_index": 1,
        "start": "2024-01-01T00:09:00",
        "end": "2024-01-01T00:09:00",
    }
    return client.get_timeseries_window_data("test", "/sensors", **(args | kwargs))


@pytest.mark.parametrize("variable", ["temp", "reordered"])
def test_inclusive_duplicate_times_cross_chunk_boundary(source, variable):
    """Read both equal endpoints, with only the requested measurement chunks."""
    client, ds, store, _ = source
    window = _request(client, variable=variable, depth_indices=[2, 0])
    expected = ds[variable].isel(time=slice(2, 4), site=1, depth=[2, 0])
    xr.testing.assert_identical(window, expected)
    assert store.chunk_reads(f"sensors/{variable}")[0] == 4
    assert isinstance(window.data, np.ndarray)


@pytest.mark.parametrize("variable", ["temp", "reordered"])
def test_single_depth_all_nan(source, variable):
    """An all-NaN selection retains its timestamps, dtype, and depth coordinate."""
    client, ds, _, _ = source
    result = _request(client, variable=variable, depth_indices=[2])
    expected = ds[variable].isel(time=slice(2, 4), site=1, depth=[2])
    xr.testing.assert_identical(result, expected)
    assert np.isnan(result.values).all()


def test_irregular_and_empty_windows(source):
    """Select original timestamps between bounds and return a typed empty slice in a gap."""
    client, ds, store, _ = source
    selected = _request(client, start="2024-01-01T00:01", end="2024-01-01T00:26")
    xr.testing.assert_identical(selected, ds.temp.isel(time=slice(1, 5), site=1))
    store.reads.clear()
    empty = _request(client, start="2024-01-01T00:10", end="2024-01-01T00:24")
    xr.testing.assert_identical(empty, ds.temp.isel(time=slice(4, 4), site=1))
    assert store.chunk_reads("sensors/temp") == (0, 0)


def test_window_preserves_bound_precision(source, monkeypatch):
    """Bounds with fractional seconds select exact instants on a second-resolution axis."""
    client, ds, _, _ = source
    ds = ds.assign_coords(time=ds.indexes["time"].as_unit("s"))
    monkeypatch.setattr(client, "_get_group", lambda *args: SimpleNamespace(dataset=ds))
    result = _request(
        client, start="2024-01-01T00:09:00.001Z", end="2024-01-01T00:25:00"
    )
    xr.testing.assert_identical(result, ds.temp.isel(time=slice(4, 5), site=1))


def test_window_bounds_outside_index_dtype_range(source, monkeypatch):
    """Valid timestamps beyond the index's representable range can bound a window."""
    client, ds, _, _ = source
    ds = ds.assign_coords(time=ds.indexes["time"].as_unit("ns"))
    monkeypatch.setattr(client, "_get_group", lambda *args: SimpleNamespace(dataset=ds))
    result = _request(client, start="1600-01-01", end="3000-01-01")
    xr.testing.assert_identical(result, ds.temp.isel(site=1))


def test_depthless_array(source):
    """A measurement with no depth dimension returns a one-dimensional array."""
    client, ds, _, _ = source
    xr.testing.assert_identical(
        _request(client, variable="air"), ds.air.isel(time=slice(2, 4), site=1)
    )
    with pytest.raises(ValueError, match="depth"):
        _request(client, variable="air", depth_indices=[0])


def test_empty_depth_selection(source):
    """An explicitly empty selection retains time but reads no measurements."""
    client, ds, store, _ = source
    actual = _request(client, depth_indices=[])
    xr.testing.assert_identical(
        actual, ds.temp.isel(time=slice(2, 4), site=1, depth=[])
    )
    assert store.chunk_reads("sensors/temp") == (0, 0)


@pytest.mark.parametrize(
    ("args", "error", "message"),
    [
        ({"start": "2024-01-02"}, ValueError, "start"),
        ({"end": "NaT"}, ValueError, "NaT"),
        ({"entity_index": -1}, IndexError, "entity"),
        ({"depth_indices": [3]}, IndexError, "depth"),
    ],
)
def test_invalid_selection(source, args, error, message):
    """Invalid bounds and indices fail explicitly."""
    client, _, _, _ = source
    with pytest.raises(error, match=message):
        _request(client, **args)


def test_cache_reuses_group_metadata_and_isolates_results(source):
    """Normalize equivalent bounds, isolate cached data, and reopen on invalidation."""
    client, ds, store, opens = source
    window = _request(client, depth_indices=[0])
    window.values[:] = -999
    window.coords["time"].values[:] = np.datetime64("1970-01-01")
    window.coords["depth"].values[:] = -999
    store.reads.clear()
    again = _request(
        client, depth_indices=(0,), start=pd.Timestamp("2024-01-01T01:09+01:00")
    )
    xr.testing.assert_identical(
        again, ds.temp.isel(time=slice(2, 4), site=1, depth=[0])
    )
    assert store.reads == []
    _request(client, depth_indices=[0], entity_index=0)
    _request(client, variable="reordered", depth_indices=[0])
    _request(client, depth_indices=[1])
    _request(client, depth_indices=[1], start="2024-01-01T00:00")
    assert len(opens) == 1
    client.clear_cache()
    _request(client, depth_indices=[0])
    assert len(opens) == 2


def test_invalid_time_axis(source, monkeypatch):
    """Reject unsorted datetime coordinates before any measurement reads."""
    client, ds, _, _ = source
    unsorted = ds.assign_coords(time=ds.unsorted.values)
    monkeypatch.setattr(
        client, "_get_group", lambda *args: SimpleNamespace(dataset=unsorted)
    )
    with pytest.raises(ValueError, match="ascending"):
        _request(client)


def test_metadata_cache_isolated_by_view_and_group(source):
    """Aliases for one group share metadata, while different views and groups reopen."""
    client, _, _, opens = source
    args = {
        "variable": "temp",
        "entity_index": 1,
        "start": "2024-01-01",
        "end": "2024-01-02",
    }
    client.get_timeseries_window_data("first", "/sensors/", **args)
    client.get_timeseries_window_data("first", "sensors", **args)
    client.get_timeseries_window_data("second", "sensors", **args)
    client.get_timeseries_window_data("first", "another", **args)
    assert len(opens) == 3


def test_concurrent_request_cannot_fill_new_generation(source, monkeypatch):
    """A read finishing after reload can return to its caller without warming fresh caches."""
    client, ds, store, opens = source
    entered, release = Event(), Event()
    original = xr.DataArray.load

    def pause(array, **kwargs):
        if array.name == "temp" and not entered.is_set():
            entered.set()
            assert release.wait(10)
        return original(array, **kwargs)

    monkeypatch.setattr(xr.DataArray, "load", pause)
    with ThreadPoolExecutor(max_workers=2) as pool:
        pending = pool.submit(_request, client, depth_indices=[0])
        try:
            assert entered.wait(10)
            other = pool.submit(_request, client, depth_indices=[1]).result(10)
            xr.testing.assert_identical(
                other, ds.temp.isel(time=slice(2, 4), site=1, depth=[1])
            )
            client.clear_cache()
        finally:
            release.set()
        pending.result(10)

    store.reads.clear()
    _request(client, depth_indices=[0])
    assert len(opens) == 2
    assert store.chunk_reads("sensors/temp")[0] == 2, store.reads
