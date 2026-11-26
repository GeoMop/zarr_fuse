from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from zarr.storage import LocalStore

from dashboard import log_buffer
from dashboard.config import SchemaConfig, SchemaFieldsConfig
from dashboard.data import LocalClient
from dashboard.plot_selection import SelectionState
from dashboard.scripts.benchmark_timeseries import CountingStore


@pytest.fixture(autouse=True)
def clean_log_buffer() -> None:
    log_buffer.clear()
    yield
    log_buffer.clear()


@pytest.fixture
def setup_right(tmp_path, monkeypatch):
    import holoviews as hv

    hv.extension("bokeh")
    times = pd.date_range("2024-01-01", periods=1041, freq="h").delete(981)
    times = times.insert(984, times[983])
    values = np.arange(len(times) * 4, dtype=np.float32).reshape(len(times), 2, 2)
    values[989, 0, 0] = np.nan
    values[:, 1, 1] = np.nan
    ds = xr.Dataset(
        {"temp": (("depth", "time", "entity"), values.transpose(2, 0, 1))},
        coords={"time": times, "entity": ["A", "B"], "depth": [0.0, 3.0]},
    )
    path = tmp_path / "test.zarr"
    ds.to_zarr(path, consolidated=False, encoding={"temp": {"chunks": (1, 16, 1)}})
    store = CountingStore(LocalStore(path, read_only=True))
    client = LocalClient(Path("unused.yaml"))
    monkeypatch.setattr(
        client,
        "_overview_source",
        lambda handle: (handle.store_uri, LocalStore(path, read_only=True)),
    )
    fields = SchemaFieldsConfig("lat", "lon", "time", "entity", "depth")
    schema = SchemaConfig(file="", fields=fields)
    monkeypatch.setattr(
        client, "_view_config", lambda *_: SimpleNamespace(schema=schema)
    )
    monkeypatch.setattr(
        client,
        "_get_group",
        lambda *_: SimpleNamespace(dataset=xr.open_zarr(store, consolidated=False)),
    )
    monkeypatch.setattr(client, "get_variable_metadata", lambda *_: {})
    monkeypatch.setattr(
        client,
        "get_view",
        lambda *_: {
            "defaults": {},
            "schema_display": {"entity_name": "Site"},
            "schema": {
                "fields": {"time": "time", "entity": "entity", "vertical": "depth"}
            },
            "visualization": {
                "timeseries": {"middle_window_days": 30, "right_window_hours": 24}
            },
        },
    )
    state = SelectionState()
    state.add_site(0, "A", [0.0, 3.0], times)
    data = SimpleNamespace(
        client=client, view_name="view", group_path="/", display_variable="temp"
    )
    with store:
        yield ds, data, state, store
