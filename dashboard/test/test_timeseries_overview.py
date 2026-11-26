from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from dashboard.data import LocalClient, ViewHandle
from dashboard.timeseries_overview import extrema_indices


def test_reduction_preserves_peaks_and_missing_runs():
    times = pd.Timestamp("2024-01-01") + pd.to_timedelta(
        [0, 1, 1, 3, 5, 6, 7, 10, 11], unit="s"
    )
    values = np.array([0, 12, -7, 0, np.nan, np.nan, 0, 5, 0])
    assert extrema_indices(times, values, 1).tolist() == [0, 1, 2, 3, 4, 6, 7, 8]


def test_preprocessor_cannot_create_a_missing_source(tmp_path, monkeypatch):
    """A bad source path stays missing when the preprocessing reader opens it."""
    monkeypatch.delenv("ZF_STORE_URL", raising=False)
    missing = tmp_path / "missing.zarr"
    schema = (
        Path(__file__).parents[1]
        / "test_deployment/databuk/config/schemas/bukov_schema.yaml"
    )
    handle = ViewHandle("test", schema, str(missing))
    _, store = LocalClient(tmp_path / "views.yaml")._overview_source(handle)
    assert store.read_only
    with store, pytest.raises(FileNotFoundError):
        xr.open_zarr(store, consolidated=False)
    assert not missing.exists()


def test_saved_overview_and_availability_without_raw_measurements(
    setup_right, tmp_path, monkeypatch
):
    ds, data, _, raw_store = setup_right
    client = data.client
    monkeypatch.setattr(
        client, "_view_handle", lambda *_: SimpleNamespace(store_uri="source")
    )
    monkeypatch.setenv("ZF_TIMESERIES_OVERVIEW_DIR", str(tmp_path))
    args = {
        "variable": "temp",
        "entity_index": 1,
        "depth_indices": (1, 0),
        "start": ds.indexes["time"][0],
        "end": ds.indexes["time"][-1],
    }
    assert client.get_timeseries_overview_data("view", "/", **args) is None
    path = client.generate_timeseries_overview("view", "/", "temp", tmp_path, bins=3)
    assert path.is_dir()
    with pytest.raises(FileExistsError):
        client.generate_timeseries_overview("view", "/", "temp", tmp_path, bins=3)
    client.clear_cache()
    raw_store.reads.clear()
    site = client.get_timeseries_site_metadata(
        "view", "/", variable="temp", entity_index=1
    )
    assert list(site["availability"]) == [True, False]
    summary = client.get_timeseries_overview_data("view", "/", **args)
    for depth, (times, values) in summary.items():
        raw = ds.temp.isel(entity=1, depth=depth).values
        retained = extrema_indices(ds.indexes["time"], raw, 3)
        np.testing.assert_array_equal(times, ds.indexes["time"][retained])
        np.testing.assert_array_equal(values, raw[retained])
    assert raw_store.chunk_reads("temp") == (0, 0)

    # Append/reordering changes the snapshot fingerprint without opening measurement values.
    changed = ds.assign_coords(time=ds.indexes["time"] + pd.Timedelta(hours=1))
    monkeypatch.setattr(
        client, "_get_group", lambda *_: SimpleNamespace(dataset=changed)
    )
    client.clear_cache()
    assert client.get_timeseries_overview_data("view", "/", **args) is None
    assert (
        client.get_timeseries_site_metadata(
            "view", "/", variable="temp", entity_index=1
        )["availability"]
        is None
    )
    assert raw_store.chunk_reads("temp") == (0, 0)


def test_missing_overview_keeps_unknown_depth_selectable(setup_right):
    _, _, state, _ = setup_right
    state.add_site(
        1,
        "B",
        [0, 3],
        [],
        pd.DatetimeIndex(["2024-01-01", "2024-02-01"]),
        single_trace=False,
        availability=None,
    )
    assert not state.site_depth_all_nan("B", 0)
    assert ("B", 0.0) in state.checked_combinations()
    state.update_site_data(
        1,
        [],
        state.sites[-1]["times"],
        [0, 3],
        single_trace=False,
        availability=[False, True],
    )
    assert state.site_depth_all_nan("B", 0)
    assert not state.site_depth_all_nan("B", 3)
