from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
from typing import Any

import numpy as np

# Allow running this file directly via "python dashboard/test/check_endpoint_stores.py".
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv

from dashboard.config import load_endpoints
from dashboard.data import LocalClient


def list_group_paths(structure: dict, prefix: str = "") -> list[str]:
    paths: list[str] = []
    path = structure.get("path") or "/"
    name = structure.get("name") or "root"
    paths.append(f"{prefix}{name} ({path})")
    for child in structure.get("children", []) or []:
        paths.extend(list_group_paths(child, prefix + "  "))
    return paths


def _format_sample(values: np.ndarray, limit: int = 6) -> str:
    flat = values.ravel()
    clipped = flat[:limit]
    rendered = ", ".join(repr(v) for v in clipped)
    suffix = " ..." if flat.size > limit else ""
    return f"[{rendered}{suffix}]"


def inspect_group_values(client: LocalClient, endpoint_name: str, group_path: str) -> None:
    node = client._get_group(endpoint_name, group_path)
    ds = node.dataset

    print("\n" + "=" * 72)
    print(f"Value check: endpoint='{endpoint_name}', group='{group_path}'")
    print(f"Dataset variables: {len(ds.data_vars)}")
    print(f"Dataset coords: {len(ds.coords)}")

    if len(ds.data_vars) == 0:
        print("No data variables in this node.")
        return

    print("Variables:")
    for var_name, data_array in ds.data_vars.items():
        dims = tuple(data_array.dims)
        shape = tuple(data_array.shape)

        indexers: dict[str, Any] = {
            dim: slice(0, min(int(size), 3)) for dim, size in data_array.sizes.items()
        }

        try:
            sample = np.array(data_array.isel(indexers).values)
        except Exception as exc:  # noqa: BLE001
            print(f"  - {var_name}: dims={dims}, shape={shape}, sample_error={type(exc).__name__}: {exc}")
            continue

        sample_size = int(sample.size)
        dtype = str(sample.dtype)

        if sample_size == 0:
            print(f"  - {var_name}: dims={dims}, shape={shape}, dtype={dtype}, sample=empty")
            continue

        if np.issubdtype(sample.dtype, np.number):
            finite_mask = np.isfinite(sample)
            finite_count = int(finite_mask.sum())
            print(
                f"  - {var_name}: dims={dims}, shape={shape}, dtype={dtype}, "
                f"sample_finite={finite_count}/{sample_size}, sample={_format_sample(sample)}"
            )
        else:
            non_null_count = int(np.sum(sample != None))  # noqa: E711
            print(
                f"  - {var_name}: dims={dims}, shape={shape}, dtype={dtype}, "
                f"sample_non_null={non_null_count}/{sample_size}, sample={_format_sample(sample)}"
            )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check endpoint store reachability and print group structure."
    )
    parser.add_argument(
        "--endpoints",
        default="app/databuk/config/endpoints.yaml",
        help="Path to endpoints.yaml",
    )
    parser.add_argument(
        "--env-file",
        default="dashboard/scripts/.env",
        help="Path to .env file with S3 credentials",
    )
    parser.add_argument(
        "--check-values-endpoint",
        default=None,
        help="Endpoint name for value check (e.g. profiles or wells)",
    )
    parser.add_argument(
        "--check-values-group",
        default=None,
        help="Group path for value check (e.g. Uhelna/profiles)",
    )
    args = parser.parse_args()

    load_dotenv(args.env_file)

    endpoints_path = Path(args.endpoints)
    endpoints = load_endpoints(endpoints_path)
    client = LocalClient(endpoints_path)

    print(f"Using endpoints file: {endpoints_path}")
    print(f"Found endpoints: {', '.join(endpoints.keys())}")

    for endpoint_name, endpoint_cfg in endpoints.items():
        print("\n" + "-" * 72)
        print(f"Endpoint: {endpoint_name}")
        print(f"URI: {endpoint_cfg.source.uri}")
        try:
            structure = client.get_structure(endpoint_name)
            print("Reachable: yes")
            print("Groups:")
            for line in list_group_paths(structure):
                print(f"  {line}")
        except Exception as exc:  # noqa: BLE001
            print("Reachable: no")
            print(f"Error: {type(exc).__name__}: {exc}")

    if args.check_values_endpoint and args.check_values_group:
        inspect_group_values(
            client,
            endpoint_name=args.check_values_endpoint,
            group_path=args.check_values_group,
        )
    elif args.check_values_endpoint or args.check_values_group:
        print("\nValue check skipped: provide both --check-values-endpoint and --check-values-group.")

    return 0


if __name__ == "__main__":
    code = main()
    sys.stdout.flush()
    sys.stderr.flush()
    # s3fs/aiobotocore can emit shutdown traceback on interpreter teardown in this environment.
    # Fast exit keeps script output clean after a successful check.
    os._exit(code)
