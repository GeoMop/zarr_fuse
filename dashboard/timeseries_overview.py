"""Build a single coarse, peak-preserving overview in a separate Zarr store."""

from __future__ import annotations

import hashlib
import os
from itertools import pairwise
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd
import xarray as xr


def extrema_indices(
    times: pd.DatetimeIndex, values: np.ndarray, bins: int
) -> np.ndarray:
    """Keep first, last, min and max in each time bin within each finite run."""
    if len(values) <= bins or len(values) == 0:
        return np.arange(len(values))
    ticks = times.asi8
    bucket = np.searchsorted(np.linspace(ticks[0], ticks[-1], bins + 1)[1:-1], ticks)
    finite = np.isfinite(values)
    split = (finite[1:] != finite[:-1]) | (finite[1:] & (bucket[1:] != bucket[:-1]))
    edges = np.r_[0, np.flatnonzero(split) + 1, len(values)]
    keep = []
    for start, end in pairwise(edges):
        if finite[start]:
            segment = values[start:end]
            keep.extend(
                (start, start + segment.argmin(), start + segment.argmax(), end - 1)
            )
        else:
            keep.append(start)
    return np.unique(keep)


def overview_path(
    directory: Path, store_uri: str, group: str, array: xr.DataArray
) -> Path:
    """Name a sidecar by source identity, axes, and operator-provided revision."""
    digest = hashlib.sha256()
    digest.update(
        repr(
            (
                1,
                store_uri,
                group.strip("/"),
                array.name,
                array.dims,
                array.shape,
                str(array.dtype),
                os.getenv("ZF_TIMESERIES_OVERVIEW_REVISION", ""),
            )
        ).encode()
    )
    for dimension in array.dims:
        if dimension in array.indexes:
            digest.update(
                pd.util.hash_array(array.indexes[dimension].to_numpy()).tobytes()
            )
    return directory / f"{digest.hexdigest()}.zarr"


def write_overview(
    array: xr.DataArray,
    *,
    time_dim: str,
    entity_dim: str,
    depth_dim: str | None,
    destination: Path,
    bins: int = 750,
) -> None:
    """Read one entity at a time and publish its compact overview atomically."""
    if bins < 1:
        raise ValueError("bins must be positive")
    if destination.exists():
        raise FileExistsError(destination)
    times = array.indexes[time_dim]
    if (
        not isinstance(times, pd.DatetimeIndex)
        or times.hasnans
        or not times.is_monotonic_increasing
    ):
        raise ValueError("Overview requires an ascending datetime index without NaT")
    if depth_dim not in array.dims:
        depth_dim = None
    depths = 1 if depth_dim is None else array.sizes[depth_dim]
    shape = (array.sizes[entity_dim], depths)
    starts = np.zeros(shape, dtype=np.int64)
    lengths = np.zeros(shape, dtype=np.int64)
    available = np.zeros(shape, dtype=bool)
    point_times, point_values = [], []
    offset = 0
    ticks = times.to_numpy(dtype=f"datetime64[{times.unit}]")
    for entity in range(shape[0]):
        selected = array.isel({entity_dim: entity})
        values = (
            selected.transpose(time_dim).values[:, None]
            if depth_dim is None
            else selected.transpose(time_dim, depth_dim).values
        )
        for depth, trace in enumerate(values.T):
            retained = extrema_indices(times, trace, bins)
            starts[entity, depth] = offset
            lengths[entity, depth] = len(retained)
            available[entity, depth] = np.isfinite(trace).any()
            offset += len(retained)
            point_times.append(ticks[retained])
            point_values.append(trace[retained])
    dimensions = ("entity_position", "depth_position")
    summary = xr.Dataset(
        {
            "start": (dimensions, starts),
            "length": (dimensions, lengths),
            "available": (dimensions, available),
            "timestamp": (
                "point",
                np.concatenate(point_times) if point_times else ticks[:0],
            ),
            "value": (
                "point",
                np.concatenate(point_values)
                if point_values
                else np.empty(0, dtype=array.dtype),
            ),
        }
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=destination.parent) as tmp:
        staging = Path(tmp) / "overview.zarr"
        summary.to_zarr(
            staging,
            mode="w",
            consolidated=False,
            zarr_format=3,
            encoding={key: {"chunks": (2048,)} for key in ("timestamp", "value")},
        )
        staging.rename(destination)
