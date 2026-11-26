"""Regression tests for replacing LocalClient caches instead of clearing them."""

from __future__ import annotations

import threading
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from dashboard import data as data_module
from dashboard.data import LocalClient


def _make_dataset() -> xr.Dataset:
    return xr.Dataset(
        {"temp": (("time", "entity", "vertical"), np.arange(8.0).reshape(2, 2, 2))},
        coords={
            "time": pd.date_range("2020-01-01", periods=2),
            "entity": ["A", "B"],
            "vertical": [0.0, 1.0],
            "lat": ("entity", [47.0, 48.0]),
            "lon": ("entity", [11.0, 12.0]),
        },
    )


def _make_client(monkeypatch: pytest.MonkeyPatch) -> tuple[LocalClient, dict[str, int]]:
    dataset = _make_dataset()
    calls = {"get_group": 0}
    view = SimpleNamespace(
        defaults=SimpleNamespace(display_variable="temp"),
        schema=object(),
    )

    def _view_config(self, view_name):
        return view

    def _get_group(self, view_name, group_path):
        calls["get_group"] += 1
        return SimpleNamespace(dataset=dataset)

    monkeypatch.setattr(LocalClient, "_view_config", _view_config)
    monkeypatch.setattr(LocalClient, "_get_group", _get_group)
    monkeypatch.setattr(
        data_module,
        "resolve_schema_fields",
        lambda schema, group_path: SimpleNamespace(
            lat="lat", lon="lon", time="time", vertical="vertical", entity="entity"
        ),
    )
    return LocalClient(Path("unused.yml")), calls


def _pause_before_cache_insert(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[threading.Event, threading.Event]:
    entered = threading.Event()
    release = threading.Event()
    original = data_module._to_json_floats

    def _blocking(values):
        if not entered.is_set():
            entered.set()
            if not release.wait(timeout=5):
                raise AssertionError("test did not release blocked request")
        return original(values)

    monkeypatch.setattr(data_module, "_to_json_floats", _blocking)
    return entered, release


def test_clear_cache_replaces_active_caches() -> None:
    client = LocalClient(Path("unused.yml"))
    old_map = client._map_data_cache
    old_window_cache = client._timeseries_window_cache
    old_map["map"] = 1

    client.clear_cache()

    assert client._map_data_cache == {} and client._map_data_cache is not old_map
    assert client._timeseries_window_cache is not old_window_cache


def test_inflight_map_request_cannot_repopulate_replacement_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _make_client(monkeypatch)
    entered, release = _pause_before_cache_insert(monkeypatch)
    results: list[dict] = []
    errors: list[BaseException] = []

    def _request() -> dict:
        return client.get_map_data(
            "view", "/", variable="temp", time_index=0, depth_index=0
        )

    def _worker() -> None:
        try:
            results.append(_request())
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()
    try:
        assert entered.wait(timeout=5)
        client.clear_cache()
        new_cache = client._map_data_cache
    finally:
        release.set()
    thread.join(timeout=5)

    assert not thread.is_alive()
    if errors:
        raise errors[0]
    assert results and results[0]["status"] == "success"
    assert new_cache == {}
    assert client._map_data_cache is new_cache
    assert calls["get_group"] == 1

    second = _request()
    assert second["status"] == "success"
    assert calls["get_group"] == 2
    assert len(client._map_data_cache) == 1

    third = _request()
    assert third == second
    assert calls["get_group"] == 2
