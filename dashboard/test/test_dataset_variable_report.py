from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from dashboard.config import load_endpoints
from dashboard.data import LocalClient


def _collect_group_paths(structure: dict[str, Any]) -> list[str]:
    paths: list[str] = []

    def walk(node: dict[str, Any]) -> None:
        path = node.get("path") or "/"
        paths.append(path)
        for child in node.get("children", []) or []:
            walk(child)

    walk(structure)
    return paths


def _resolve_endpoints_path() -> Path | None:
    env_path = os.getenv("ENDPOINTS_PATH")
    if env_path:
        path = Path(env_path).expanduser().resolve()
        if path.exists():
            return path

    repo_root = Path(__file__).resolve().parents[2]
    candidates = [
        repo_root / "dashboard" / "config" / "endpoints.yaml",
        repo_root / "config" / "endpoints.yaml",
        repo_root / "app" / "databuk" / "config" / "endpoints.yaml",
        repo_root / "app" / "databuk" / "dashboard" / "backend" / "config" / "endpoints.yaml",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()

    return None


def _count_missing_values(values: np.ndarray) -> int:
    if values.size == 0:
        return 0

    dtype = values.dtype

    if np.issubdtype(dtype, np.floating) or np.issubdtype(dtype, np.complexfloating):
        return int(np.isnan(values).sum())

    if np.issubdtype(dtype, np.datetime64) or np.issubdtype(dtype, np.timedelta64):
        return int(np.isnat(values).sum())

    # Object/string/mixed arrays: count None and NaN-like values safely.
    missing = 0
    for item in values.ravel():
        if item is None:
            missing += 1
            continue
        if isinstance(item, (float, np.floating)) and np.isnan(item):
            missing += 1
    return missing


def _infer_dependencies(data_array, dataset) -> dict[str, Any]:
    attr_keys = (
        "coordinates",
        "ancillary_variables",
        "bounds",
        "grid_mapping",
        "formula_terms",
        "cell_measures",
        "geometry",
        "node_coordinates",
        "mesh",
        "edge_coordinates",
        "face_coordinates",
    )

    candidate_names = set(data_array.dims)
    token_re = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

    for key in attr_keys:
        raw = data_array.attrs.get(key)
        if isinstance(raw, str):
            candidate_names.update(token_re.findall(raw))

    coord_dependencies = sorted(name for name in candidate_names if name in dataset.coords)
    variable_dependencies = sorted(
        name for name in candidate_names if name in dataset.data_vars and name != data_array.name
    )

    return {
        "depends_on_dimensions": list(data_array.dims),
        "depends_on_coordinates": coord_dependencies,
        "depends_on_other_variables": variable_dependencies,
        "depends_on_any_other_variable": bool(variable_dependencies),
    }


def _build_variable_record(endpoint_name: str, group_path: str, variable_name: str, data_array, dataset) -> dict[str, Any]:
    values = np.asarray(data_array.values)
    total_count = int(values.size)
    nan_count = _count_missing_values(values)

    record = {
        "endpoint": endpoint_name,
        "group_path": group_path,
        "variable": variable_name,
        "dtype": str(data_array.dtype),
        "dims": list(data_array.dims),
        "shape": list(data_array.shape),
        "size": total_count,
        "nan_count": nan_count,
        "non_nan_count": total_count - nan_count,
        "attrs": {str(k): str(v) for k, v in data_array.attrs.items()},
    }
    record.update(_infer_dependencies(data_array, dataset))
    return record


def _build_dataset_report(endpoints_path: Path) -> dict[str, Any]:
    endpoints = load_endpoints(endpoints_path)
    client = LocalClient(endpoints_path)

    report: dict[str, Any] = {
        "endpoints_path": str(endpoints_path),
        "summary": {
            "endpoints_total": len(endpoints),
            "groups_total": 0,
            "variables_total": 0,
        },
        "variables": [],
    }

    for endpoint_name in endpoints:
        structure = client.get_structure(endpoint_name)
        group_paths = _collect_group_paths(structure)
        report["summary"]["groups_total"] += len(group_paths)

        for group_path in group_paths:
            node = client._get_group(endpoint_name, group_path)
            dataset = node.dataset
            for variable_name, data_array in dataset.data_vars.items():
                report["variables"].append(
                    _build_variable_record(endpoint_name, group_path, variable_name, data_array, dataset)
                )

    report["summary"]["variables_total"] = len(report["variables"])
    return report


def test_dataset_variable_report() -> None:
    endpoints_path = _resolve_endpoints_path()
    if endpoints_path is None:
        pytest.skip("No endpoints.yaml found. Set ENDPOINTS_PATH or add a known endpoints config.")

    report = _build_dataset_report(endpoints_path)

    output_path = os.getenv("DASHBOARD_VARIABLE_REPORT_OUT")
    if output_path:
        target = Path(output_path).expanduser().resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(json.dumps(report, indent=2))
    assert report["summary"]["endpoints_total"] > 0
