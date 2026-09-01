"""Scan a Zarr store as general store health report (like ``df.info()``).

Opens the store described by ``SCHEMA_PATH`` (a zarr-fuse schema YAML that
carries the ``STORE_URL`` and the store structure) and reports, for every
coordinate and data variable, its dtype, dims, shape, size and the number of
missing values (NaN / NaT / None / empty string).  The optional ``GROUP_PATH``
constant lets you inspect a specific data group instead of the store root.

S3 credentials are read from the ``ZF_S3_ACCESS_KEY`` / ``ZF_S3_SECRET_KEY`` /
``ZF_S3_ENDPOINT_URL`` environment variables (with ``S3_*`` fallback) so they
are not committed to the repository.

Configuration (fill in per project, no hardcoded store specifics):
    SCHEMA_PATH   - path to the zarr-fuse schema YAML for the store
    GROUP_PATH    - optional data group to inspect (default: store root "")

Usage:

    python dashboard/scripts/scan_store_health.py
    python dashboard/scripts/scan_store_health.py --schema path/to/schema.yaml
    python dashboard/scripts/scan_store_health.py --group bukov
    python dashboard/scripts/scan_store_health.py --verbose
    python dashboard/scripts/scan_store_health.py --only-missing
    python dashboard/scripts/scan_store_health.py --data-vars full
    python dashboard/scripts/scan_store_health.py --json report.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))

import zarr_fuse as zf



SCHEMA_PATH = "dashboard/scripts/bukov_temperatures_schema.yaml"
GROUP_PATH = "bukov"


def _env(*names: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return ""


S3_ACCESS_KEY = _env("ZF_S3_ACCESS_KEY", "S3_ACCESS_KEY")
S3_SECRET_KEY = _env("ZF_S3_SECRET_KEY", "S3_SECRET_KEY")
S3_ENDPOINT_URL = _env("ZF_S3_ENDPOINT_URL", "S3_ENDPOINT_URL")


def _mask_secret(value: str | None) -> str:
    if not value:
        return "<blank>"
    if len(value) <= 6:
        return "***"
    return f"{value[:3]}***{value[-3:]}"


def _count_missing_values(values: np.ndarray) -> int:
    if values.size == 0:
        return 0
    dtype = values.dtype
    if np.issubdtype(dtype, np.floating) or np.issubdtype(dtype, np.complexfloating):
        return int(np.isnan(values).sum())
    if np.issubdtype(dtype, np.datetime64) or np.issubdtype(dtype, np.timedelta64):
        return int(np.isnat(values).sum())
    missing = 0
    for item in values.ravel():
        if item is None:
            missing += 1
            continue
        if isinstance(item, str) and item == "":
            missing += 1
            continue
        if isinstance(item, (float, np.floating)) and np.isnan(item):
            missing += 1
    return missing


def _coord_role(name: str) -> str | None:
    lower = name.lower()
    for role in ("time", "lat", "lon", "depth", "x", "y", "z"):
        if role in lower:
            return role
    return None


def _format_sample(values: np.ndarray, limit: int = 6) -> str:
    flat = values.ravel()
    clipped = flat[:limit]
    rendered = ", ".join(repr(v) for v in clipped)
    suffix = " ..." if flat.size > limit else ""
    return f"[{rendered}{suffix}]"


def _open_dataset(schema_path: Path, group_path: str):
    creds = {}
    if S3_ACCESS_KEY:
        creds["S3_ACCESS_KEY"] = S3_ACCESS_KEY
    if S3_SECRET_KEY:
        creds["S3_SECRET_KEY"] = S3_SECRET_KEY
    if S3_ENDPOINT_URL:
        creds["S3_ENDPOINT_URL"] = S3_ENDPOINT_URL
    node = zf.open_store(schema_path, MODE="r", **creds)
    for part in [p for p in group_path.strip("/").split("/") if p]:
        node = node.children[part]
    return node.dataset


def _scan_coordinates(ds, show_samples: bool, only_missing: bool) -> list[dict]:
    rows = []
    for name, coord in ds.coords.items():
        values = np.asarray(coord.values)
        missing = _count_missing_values(values)
        record = {
            "name": str(name),
            "role": _coord_role(str(name)),
            "dtype": str(coord.dtype),
            "dims": list(coord.dims),
            "shape": list(coord.shape),
            "size": int(values.size),
            "missing": missing,
            "any_missing": missing > 0,
        }
        if only_missing and missing == 0:
            continue
        if show_samples or missing > 0:
            record["sample"] = _format_sample(values)
        rows.append(record)
    return rows


def _scan_data_vars(ds, full: bool, only_missing: bool) -> list[dict]:
    rows = []
    for name, data_array in ds.data_vars.items():
        values = np.asarray(data_array.values)
        missing = _count_missing_values(values)
        record = {
            "name": str(name),
            "dtype": str(data_array.dtype),
            "dims": list(data_array.dims),
            "shape": list(data_array.shape),
            "size": int(values.size),
            "missing": missing,
        }
        if only_missing and missing == 0:
            continue
        if full:
            record["attrs"] = {str(k): str(v) for k, v in data_array.attrs.items()}
        rows.append(record)
    return rows


def _print_header(schema_path: Path, group_path: str) -> None:
    print(f"SCHEMA: {schema_path}")
    print(f"GROUP:  {group_path or '/ (root)'}")
    print(
        "CREDENTIALS: "
        f"key={_mask_secret(S3_ACCESS_KEY)} "
        f"secret={_mask_secret(S3_SECRET_KEY)} "
        f"endpoint={S3_ENDPOINT_URL or '<blank>'}"
    )


def _print_coordinates(rows: list[dict]) -> None:
    print("\nCOORDINATES")
    print("-" * 82)
    if not rows:
        print("  (none matching current filters)")
        return
    header = f"{'COORD':<16}{'ROLE':<8}{'DTYPE':<18}{'SHAPE':<22}{'SIZE':<8}{'MISSING':<9}ANY?"
    print(header)
    print("-" * 82)
    for row in rows:
        shape = "x".join(str(d) for d in row["shape"])
        print(
            f"{row['name']:<16}{str(row['role'] or '-'):<8}{row['dtype']:<18}"
            f"{shape:<22}{row['size']:<8}{row['missing']:<9}"
            f"{'YES' if row['any_missing'] else 'no'}"
        )
    for row in rows:
        if "sample" not in row:
            continue
        print(f"  detail {row['name']}: {row['sample']} dtype={row['dtype']} shape={'x'.join(str(d) for d in row['shape'])}")


def _print_data_vars(rows: list[dict], full: bool) -> None:
    print("\nDATA VARIABLES")
    print("-" * 72)
    if not rows:
        print("  (none matching current filters)")
        return
    header = f"{'VARIABLE':<20}{'DTYPE':<18}{'SHAPE':<24}{'MISSING'}"
    print(header)
    print("-" * 72)
    for row in rows:
        shape = "x".join(str(d) for d in row["shape"])
        print(f"{row['name']:<20}{row['dtype']:<18}{shape:<24}{row['missing']}")
    if full:
        for row in rows:
            if "attrs" not in row or not row["attrs"]:
                continue
            print(f"  detail {row['name']} attrs: {json.dumps(row['attrs'])}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Scan a Zarr store health (coords + data vars).")
    parser.add_argument("--schema", default=SCHEMA_PATH, help="Path to zarr-fuse schema YAML (default: SCHEMA_PATH constant).")
    parser.add_argument("--group", default=GROUP_PATH, help="Data group to inspect (default: store root).")
    parser.add_argument("--verbose", action="store_true", help="Show value samples for every coordinate.")
    parser.add_argument("--only-missing", action="store_true", help="Show only rows that contain missing values.")
    parser.add_argument("--data-vars", choices=["summary", "full"], default="summary", help="Data-variable verbosity.")
    parser.add_argument("--json", default=None, help="Write the full report to a JSON file.")
    args = parser.parse_args(argv)

    if not args.schema:
        print("No schema configured. Set SCHEMA_PATH at the top of this script or pass --schema <path>.")
        return 2
    schema_path = Path(args.schema).expanduser().resolve()
    if not schema_path.exists():
        print(f"Schema file not found: {schema_path}")
        return 2
    group_path = args.group or ""

    _print_header(schema_path, group_path)

    try:
        ds = _open_dataset(schema_path, group_path)
    except Exception as exc:  # noqa: BLE001
        print(f"\nCould not open store: {type(exc).__name__}: {exc}")
        print("Check the schema path, group, and the S3 credentials, then rerun.")
        return 1

    coord_rows = _scan_coordinates(ds, show_samples=args.verbose, only_missing=args.only_missing)
    var_rows = _scan_data_vars(ds, full=args.data_vars == "full", only_missing=args.only_missing)

    _print_coordinates(coord_rows)
    _print_data_vars(var_rows, full=args.data_vars == "full")

    if args.json or os.getenv("DASHBOARD_COORD_REPORT_OUT"):
        target = Path(args.json).expanduser().resolve() if args.json else Path(os.getenv("DASHBOARD_COORD_REPORT_OUT")).expanduser().resolve()
        report = {
            "schema": str(schema_path),
            "group": group_path or "/",
            "coordinates": coord_rows,
            "data_variables": var_rows,
        }
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"\nReport written to {target}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
