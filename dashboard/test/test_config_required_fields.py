"""Validation tests for required dashboard view configuration values.

Covers missing keys, explicit YAML ``null`` values and valid existing
configurations with root-level or group-only field mappings.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from dashboard.config import load_view_config, load_views, resolve_schema_fields

VIEW_NAME = "test_view"


def _base_view() -> dict:
    return {
        "source": {"uri": "s3://bucket/store.zarr", "schema_path": "config/test_schema.yaml"},
        "variable_map": {
            "fields": {"lat": "latitude", "lon": "longitude", "time": "date_time", "entity": "station"}
        },
        "defaults": {"group_path": "/", "display_variable": "temp"},
        "visualization": {
            "map": {"title": "Test", "point_size": 5},
            "timeseries": {"middle_window_days": 30, "right_window_hours": 24},
            "overlay": {"enabled": False},
        },
    }


def _write_view(tmp_path: Path, view: dict | None = None) -> Path:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "test_schema.yaml").write_text("{}\n", encoding="utf-8")
    data = {VIEW_NAME: view if view is not None else _base_view()}
    views_path = config_dir / "zf_view.yaml"
    views_path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return views_path


def _view_with(**mutations) -> dict:
    """Return a base view after applying dotted-path mutations.

    Example: ``_view_with(**{"variable_map.fields.lat": None})``.
    """
    view = _base_view()
    for dotted_key, value in mutations.items():
        target = view
        parts = dotted_key.split(".")
        for part in parts[:-1]:
            target = target[part]
        if value is _MISSING:
            del target[parts[-1]]
        else:
            target[parts[-1]] = value
    return view


class _Missing:
    pass


_MISSING = _Missing()


# ── valid configurations ───────────────────────────────────────────

def test_valid_root_config_loads(tmp_path: Path) -> None:
    """A root-level `fields` mapping loads into a fully populated SchemaFieldsConfig."""
    cfg = load_view_config(_write_view(tmp_path), VIEW_NAME)

    assert cfg.schema.fields is not None
    assert (cfg.schema.fields.lat, cfg.schema.fields.lon) == ("latitude", "longitude")
    assert (cfg.schema.fields.time, cfg.schema.fields.entity) == ("date_time", "station")
    assert cfg.defaults.group_path == "/"
    assert cfg.visualization.map.title == "Test"
    assert cfg.visualization.map.point_size == 5


def test_group_only_mapping_loads(tmp_path: Path) -> None:
    """Group-only views keep fields=None while preserving the group mapping and root group_path."""
    view = _base_view()
    view["variable_map"] = {
        "bukov": {"lat": "latitude", "lon": "longitude", "time": "date_time", "entity": "borehole"}
    }
    cfg = load_view_config(_write_view(tmp_path, view), VIEW_NAME)

    assert cfg.schema.fields is None
    assert set(cfg.schema.group_fields) == {"bukov"}
    assert resolve_schema_fields(cfg.schema, "bukov").entity == "borehole"
    assert cfg.defaults.group_path == "/"


def test_display_variable_is_optional(tmp_path: Path) -> None:
    """Missing display_variable is allowed: the dashboard picks the first available variable."""
    cfg = load_view_config(
        _write_view(tmp_path, _view_with(**{"defaults.display_variable": _MISSING})), VIEW_NAME
    )

    assert cfg.defaults.display_variable is None


# ── required schema field values ───────────────────────────────────

@pytest.mark.parametrize("field", ["lat", "lon", "time", "entity"])
def test_missing_required_schema_field_rejected(tmp_path: Path, field: str) -> None:
    view = _view_with(**{f"variable_map.fields.{field}": _MISSING})
    with pytest.raises(ValueError, match=f"missing required keys.*{field}"):
        load_views(_write_view(tmp_path, view))


@pytest.mark.parametrize("field", ["lat", "lon", "time", "entity"])
def test_null_required_schema_field_rejected(tmp_path: Path, field: str) -> None:
    view = _view_with(**{f"variable_map.fields.{field}": None})
    with pytest.raises(ValueError, match=f"null or empty required field.*{field}"):
        load_views(_write_view(tmp_path, view))


@pytest.mark.parametrize("field", ["lat", "lon", "time", "entity"])
def test_empty_required_schema_field_rejected(tmp_path: Path, field: str) -> None:
    view = _view_with(**{f"variable_map.fields.{field}": "  "})
    with pytest.raises(ValueError, match=f"null or empty required field.*{field}"):
        load_views(_write_view(tmp_path, view))


def test_group_mapping_missing_required_field_rejected(tmp_path: Path) -> None:
    view = _base_view()
    view["variable_map"] = {
        "bukov": {"lat": "latitude", "lon": "longitude", "time": "date_time"}
    }
    with pytest.raises(ValueError, match="variable_map.bukov.*missing required keys.*entity"):
        load_views(_write_view(tmp_path, view))


def test_variable_map_without_mapping_rejected(tmp_path: Path) -> None:
    view = _base_view()
    view["variable_map"] = {}
    with pytest.raises(ValueError, match="must define either variable_map.fields or nested group mappings"):
        load_views(_write_view(tmp_path, view))


# ── required defaults / visualization values ───────────────────────

@pytest.mark.parametrize("value", [_MISSING, None])
def test_missing_or_null_group_path_rejected(tmp_path: Path, value) -> None:
    view = _view_with(**{"defaults.group_path": value})
    with pytest.raises(ValueError, match="defaults.group_path is required and must not be null"):
        load_views(_write_view(tmp_path, view))


@pytest.mark.parametrize("value", [_MISSING, None])
def test_missing_or_null_map_title_rejected(tmp_path: Path, value) -> None:
    view = _view_with(**{"visualization.map.title": value})
    with pytest.raises(ValueError, match="visualization.map.title is required and must not be null"):
        load_views(_write_view(tmp_path, view))


@pytest.mark.parametrize("value", [_MISSING, None])
def test_missing_or_null_point_size_rejected(tmp_path: Path, value) -> None:
    view = _view_with(**{"visualization.map.point_size": value})
    with pytest.raises(ValueError, match="visualization.map.point_size is required and must not be null"):
        load_views(_write_view(tmp_path, view))


@pytest.mark.parametrize(
    "key",
    ["middle_window_days", "right_window_hours"],
)
@pytest.mark.parametrize("value", [_MISSING, None])
def test_missing_or_null_timeseries_window_rejected(tmp_path: Path, key: str, value) -> None:
    view = _view_with(**{f"visualization.timeseries.{key}": value})
    with pytest.raises(ValueError, match=rf"visualization.timeseries.{key} is required and must not be null"):
        load_views(_write_view(tmp_path, view))


# ── constructors no longer accept silently missing required values ─

def test_dataclass_constructors_require_required_fields() -> None:
    from dashboard.config import DefaultsConfig, MapConfig, SchemaFieldsConfig, TimeSeriesConfig

    with pytest.raises(TypeError):
        SchemaFieldsConfig(lat="lat", lon="lon", time="time")  # entity missing
    with pytest.raises(TypeError):
        DefaultsConfig()  # group_path missing
    with pytest.raises(TypeError):
        MapConfig(title="t")  # point_size missing
    with pytest.raises(TypeError):
        TimeSeriesConfig(middle_window_days=30)  # right_window_hours missing
