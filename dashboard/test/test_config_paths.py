"""Tests for ENDPOINTS_PATH and SCHEMAS_PATH resolution.

Validates the Kubernetes deployment path contract:
- ENDPOINTS_PATH points to a full file path
- SCHEMAS_PATH overrides schema file resolution (filename-only lookup)
- Without SCHEMAS_PATH, existing relative-path resolution is preserved

Run: python -m pytest dashboard/test/test_config_paths.py -v
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from dashboard.config import load_endpoints

_SCHEMA_CONTENT = {
    "test_group": {
        "VARS": {
            "val": {
                "unit": "m",
                "description": "test value",
                "df_col": "val",
                "coords": ["time", "site"],
            },
        },
        "COORDS": {},
    },
}


def _write_endpoints(config_dir: Path, schema_path_str: str) -> Path:
    endpoints = {
        "test_endpoint": {
            "description": "test",
            "version": "1.0.0",
            "reload_interval": 60,
            "source": {
                "type": "s3",
                "store_type": "zarr",
                "uri": "s3://bucket/store.zarr",
                "schema_path": schema_path_str,
            },
            "variable_map": {
                "test": {
                    "lat": "lat",
                    "lon": "lon",
                    "time": "time",
                    "entity": "site",
                },
            },
            "defaults": {
                "display_variable": "val",
                "group_path": "/",
            },
            "visualization": {
                "map": {
                    "title": "Test",
                    "point_size": 5,
                    "alpha": 1.0,
                },
                "timeseries": {
                    "middle_window_days": 7,
                    "right_window_hours": 24,
                },
                "overlay": {"enabled": False},
            },
        },
    }
    config_dir.mkdir(parents=True, exist_ok=True)
    ep_path = config_dir / "endpoints.yaml"
    ep_path.write_text(yaml.dump(endpoints), encoding="utf-8")
    return ep_path


def _write_schema(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(_SCHEMA_CONTENT), encoding="utf-8")


class TestWithoutSchemasPath:
    """Default behavior: schema resolved relative to config_path.parent.parent."""

    def test_schema_found_via_relative_path(self, tmp_path: Path):
        """endpoints.yaml at <tmp>/config/endpoints.yaml, schema at <tmp>/config/schemas/test_schema.yaml."""
        ep_path = _write_endpoints(tmp_path / "config", "config/schemas/test_schema.yaml")
        _write_schema(tmp_path / "config" / "schemas" / "test_schema.yaml")

        loaded = load_endpoints(ep_path)
        assert "test_endpoint" in loaded

    def test_schema_not_found_raises(self, tmp_path: Path):
        """Without SCHEMAS_PATH and no schema file, loading fails."""
        ep_path = _write_endpoints(tmp_path / "config", "config/schemas/test_schema.yaml")
        # Do NOT write the schema file

        with pytest.raises(FileNotFoundError):
            load_endpoints(ep_path)


class TestWithSchemasPath:
    """SCHEMAS_PATH overrides schema resolution to SCHEMAS_PATH / filename."""

    def test_schema_found_in_schemas_path(self, tmp_path: Path):
        """Schema file placed at SCHEMAS_PATH/test_schema.yaml is found."""
        schemas_dir = tmp_path / "mounted_schemas"
        _write_schema(schemas_dir / "test_schema.yaml")

        ep_path = _write_endpoints(tmp_path / "config", "config/schemas/test_schema.yaml")
        os.environ["SCHEMAS_PATH"] = str(schemas_dir)
        try:
            loaded = load_endpoints(ep_path)
            assert "test_endpoint" in loaded
        finally:
            os.environ.pop("SCHEMAS_PATH", None)

    def test_schema_not_in_schemas_path_raises(self, tmp_path: Path):
        """SCHEMAS_PATH set but schema file not present — load fails."""
        schemas_dir = tmp_path / "mounted_schemas"
        schemas_dir.mkdir()
        # Do NOT write the schema file

        ep_path = _write_endpoints(tmp_path / "config", "config/schemas/test_schema.yaml")
        os.environ["SCHEMAS_PATH"] = str(schemas_dir)
        try:
            with pytest.raises(FileNotFoundError):
                load_endpoints(ep_path)
        finally:
            os.environ.pop("SCHEMAS_PATH", None)

    def test_schemas_path_not_a_directory_raises(self, tmp_path: Path):
        """SCHEMAS_PATH pointing to a file raises."""
        not_a_dir = tmp_path / "file.txt"
        not_a_dir.write_text("x")

        ep_path = _write_endpoints(tmp_path / "config", "config/schemas/test_schema.yaml")
        os.environ["SCHEMAS_PATH"] = str(not_a_dir)
        try:
            with pytest.raises(FileNotFoundError, match="SCHEMAS_PATH"):
                load_endpoints(ep_path)
        finally:
            os.environ.pop("SCHEMAS_PATH", None)

    def test_schemas_path_nonexistent_raises(self, tmp_path: Path):
        """SCHEMAS_PATH pointing to a non-existent directory raises."""
        ep_path = _write_endpoints(tmp_path / "config", "config/schemas/test_schema.yaml")
        os.environ["SCHEMAS_PATH"] = str(tmp_path / "nonexistent")
        try:
            with pytest.raises(FileNotFoundError, match="SCHEMAS_PATH"):
                load_endpoints(ep_path)
        finally:
            os.environ.pop("SCHEMAS_PATH", None)


class TestKubernetesDeploymentLayout:
    """Simulate the exact Kubernetes deployment layout to catch path mismatches.

    Pod layout (hard-coded):
        /opt/hlavo/dashboard/config/endpoints.yaml  ← ENDPOINTS_PATH
        /opt/hlavo/dashboard/schemas/bukov_schema.yaml  ← SCHEMAS_PATH

    endpoints.yaml contains: schema_path: "config/schemas/bukov_schema.yaml"
    Without SCHEMAS_PATH this would resolve to
        /opt/hlavo/dashboard/config/schemas/bukov_schema.yaml  (WRONG — doesn't exist)
    With SCHEMAS_PATH=/opt/hlavo/dashboard/schemas it resolves to
        /opt/hlavo/dashboard/schemas/bukov_schema.yaml  (CORRECT)
    """

    def test_deployed_layout_with_schemas_path(self, tmp_path: Path):
        """Full pod layout — SCHEMAS_PATH makes schema resolution work."""
        pod_root = tmp_path / "opt" / "hlavo" / "dashboard"
        config_dir = pod_root / "config"
        schemas_mount = pod_root / "schemas"

        _write_schema(schemas_mount / "bukov_schema.yaml")
        ep_path = _write_endpoints(config_dir, "config/schemas/bukov_schema.yaml")

        os.environ["SCHEMAS_PATH"] = str(schemas_mount)
        try:
            loaded = load_endpoints(ep_path)
            ep = loaded["test_endpoint"]
            assert ep.schema.file == "config/schemas/bukov_schema.yaml"
        finally:
            os.environ.pop("SCHEMAS_PATH", None)

    def test_deployed_layout_without_schemas_path_fails(self, tmp_path: Path):
        """Same layout without SCHEMAS_PATH — schema resolution fails because
        <tmp>/config/schemas/ doesn't exist in this layout."""
        pod_root = tmp_path / "opt" / "hlavo" / "dashboard"
        config_dir = pod_root / "config"
        schemas_mount = pod_root / "schemas"

        _write_schema(schemas_mount / "bukov_schema.yaml")
        ep_path = _write_endpoints(config_dir, "config/schemas/bukov_schema.yaml")

        os.environ.pop("SCHEMAS_PATH", None)
        with pytest.raises(FileNotFoundError):
            load_endpoints(ep_path)
