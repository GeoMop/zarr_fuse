"""Measure an existing local Zarr time series from the repository root.

    # PowerShell
    $env:DASHBOARD_PROFILE = "1"
    python -m dashboard.scripts.benchmark_timeseries tests/zf/surface.zarr.zip --group bukov

Cold opens a fresh store and dataset. Warm repeats on that same dataset.
The dashboard payload cache and OS cache flushing are outside this benchmark.
Use --compare-readers to measure LocalClient history and window requests instead.
In that mode, repeat uses the payload cache and metadata-warm changes a bound
to keep the selection off the payload cache.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from pathlib import Path
from statistics import median
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import xarray as xr
import zarr
from zarr.storage import LocalStore, WrapperStore, ZipStore

from dashboard.config import SchemaConfig, SchemaFieldsConfig
from dashboard.data import LocalClient, _to_json_floats
from dashboard.perf_summary import count, measure_operation
from dashboard.profiling import profiling_enabled, time_stage


class CountingStore(WrapperStore):
    """Record logical GETs and the bytes returned before Zarr decoding."""

    def __init__(self, store: LocalStore | ZipStore) -> None:
        super().__init__(store)
        self.reads: list[tuple[str, int]] = []

    async def get(self, key, prototype, byte_range=None):
        """Record a single GET, including requests for absent chunks."""
        value = await super().get(key, prototype, byte_range)
        self.reads.append((key, 0 if value is None else len(value)))
        return value

    async def _get_many(self, requests):
        async for key, value in super()._get_many(requests):
            self.reads.append((key, 0 if value is None else len(value)))
            yield key, value

    def chunk_reads(self, array_path: str) -> tuple[int, int]:
        """Return request count and returned bytes for one array's data keys."""
        metadata = {"zarr.json", ".zarray", ".zattrs"}
        sizes = [
            size
            for key, size in self.reads
            if key.startswith(f"{array_path}/")
            and key.rsplit("/", 1)[-1] not in metadata
        ]
        return len(sizes), sum(sizes)


def _time_slice(index: pd.DatetimeIndex, window: str) -> slice:
    if window == "history":
        return slice(None)
    if window == "stride16":
        return slice(None, None, 16)

    start, end = _window_bounds(index, window)
    return slice(index.searchsorted(start), index.searchsorted(end, side="right"))


def _window_bounds(
    index: pd.DatetimeIndex, window: str
) -> tuple[pd.Timestamp, pd.Timestamp]:
    span = pd.Timedelta(days=30 if window == "month" else 1)
    first, last = index[0], index[-1]
    center = first + 0.95 * (last - first)
    start = max(first, min(center - span / 2, last - span))
    return start, min(last, start + span)


def _measure(
    ds: xr.Dataset, store: CountingStore, args: argparse.Namespace, window: str, hv: Any
) -> tuple[int, int, int, float, float, float, float, float]:
    store.reads.clear()
    with time_stage("benchmark.server_ready") as ready:
        index = ds.indexes[args.time]
        selected = _time_slice(index, window)
        times = index[selected]
        array = ds[args.variable].isel(
            {args.time: selected, args.entity: args.entity_index}
        )
        array = array.transpose(
            args.time, *(dim for dim in array.dims if dim != args.time)
        )
        with time_stage("benchmark.materialize") as timer:
            values = array.values
            read_ms = timer.elapsed() * 1000
        if values.ndim == 1:
            values = values[:, None]

        array_path = "/".join(filter(None, (args.group, args.variable)))
        gets, size = store.chunk_reads(array_path)
        count("measurement.gets", gets)
        count("measurement.bytes", size)
        with time_stage("benchmark.convert") as timer:
            strings = pd.to_datetime(times).astype(str).tolist()
            traces = [_to_json_floats(column) for column in values.T]
            plot_times = pd.to_datetime(strings)
            plot_values = [np.asarray(trace, dtype=float) for trace in traces]
            convert_ms = timer.elapsed() * 1000

        with time_stage("benchmark.curves") as timer:
            curves = [
                hv.Curve(
                    pd.DataFrame({"time": plot_times, "value": trace}),
                    "time",
                    "value",
                    label=f"Depth {depth}",
                )
                for depth, trace in enumerate(plot_values)
            ]
            overlay = hv.Overlay(curves)
            curve_ms = timer.elapsed() * 1000

        with time_stage("benchmark.bokeh_model") as timer:
            hv.renderer("bokeh").get_plot(overlay)
            model_ms = timer.elapsed() * 1000
        ready_ms = ready.elapsed() * 1000
    return len(times), gets, size, read_ms, convert_ms, curve_ms, model_ms, ready_ms


def _describe_sampling(index: pd.DatetimeIndex) -> None:
    intervals = (index[1:] - index[:-1]).value_counts()
    typical = intervals.head(3).to_dict()
    largest = intervals.index.max()
    print(
        f"Time span: {index[0]} to {index[-1]}; common intervals: {typical}; max gap: {largest}"
    )


def _compare_readers(
    args: argparse.Namespace, open_store: Callable[[], CountingStore]
) -> None:
    """Measure both reader APIs against the same store and entity."""
    schema = SchemaConfig(
        file="",
        fields=SchemaFieldsConfig(
            "latitude", "longitude", args.time, args.entity, args.depth
        ),
    )

    class BenchmarkClient(LocalClient):
        def __init__(self, store: CountingStore) -> None:
            super().__init__(args.store)
            self.store = store

        def _view_config(self, view_name):
            return SimpleNamespace(schema=schema)

        def _get_group(self, view_name, group_path):
            ds = xr.open_zarr(self.store, group=group_path, consolidated=False)
            # Some historical archives lack map coordinates. An explicit
            # entity index only needs their size in the legacy reader.
            for field in ("latitude", "longitude"):
                if field not in ds:
                    ds[field] = (args.entity, np.full(ds.sizes[args.entity], np.nan))
            return SimpleNamespace(dataset=ds)

    with (
        open_store() as store,
        xr.open_zarr(store, group=args.group, consolidated=False) as ds,
    ):
        time_index = ds.indexes[args.time]
    array_path = "/".join(filter(None, (args.group, args.variable)))
    timeseries_args = {
        "view_name": "benchmark",
        "group_path": args.group,
        "variable": args.variable,
        "entity_index": args.entity_index,
    }
    print("reader/cache        samples  GETs     bytes  request (ms)")
    for window in ("history", "month", "day"):
        results = {label: [] for label in ("cold", "repeat")}
        if window != "history":
            start, end = _window_bounds(time_index, window)
            results["metadata-warm"] = []
        for _ in range(args.repeats):
            with open_store() as store:
                client = BenchmarkClient(store)
                for cache, rows in results.items():
                    store.reads.clear()
                    with measure_operation(
                        "timeseries.compare", window=window, cache=cache
                    ):
                        with time_stage("benchmark.reader_request") as timer:
                            if window == "history":
                                result = client.get_timeseries_data(
                                    **timeseries_args, lat=0.0, lon=0.0
                                )
                                size = len(result["times"])
                            else:
                                request_end = (
                                    end + pd.Timedelta(1, unit="ns")
                                    if cache == "metadata-warm"
                                    else end
                                )
                                result = client.get_timeseries_window_data(
                                    **timeseries_args, start=start, end=request_end
                                )
                                size = result.sizes[args.time]
                            elapsed_ms = timer.elapsed() * 1000
                        gets, read_bytes = store.chunk_reads(array_path)
                        count("measurement.gets", gets)
                        count("measurement.bytes", read_bytes)
                    rows.append((size, gets, read_bytes, elapsed_ms))
        for cache, rows in results.items():
            size, gets, read_bytes, elapsed_ms = map(median, zip(*rows))
            print(
                f"{window + '/' + cache:<20} {size:7.0f} {gets:5.0f} {read_bytes:9.0f} {elapsed_ms:12.2f}"
            )


def main() -> None:
    """Print actual chunk layouts and median timings for the selected variable."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("store", type=Path)
    parser.add_argument("--group", default="")
    parser.add_argument("--variable", default="rock_temp")
    parser.add_argument("--time", default="date_time")
    parser.add_argument("--entity", default="borehole")
    parser.add_argument("--depth", default="depth")
    parser.add_argument("--entity-index", type=int, default=0)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--compare-readers", action="store_true")
    args = parser.parse_args()
    args.group = args.group.strip("/")
    if not profiling_enabled():
        parser.error("set DASHBOARD_PROFILE=1 before running this benchmark")
    if args.repeats < 1:
        parser.error("--repeats must be positive")

    def open_store() -> CountingStore:
        if args.store.suffix == ".zip":
            return CountingStore(ZipStore(args.store, mode="r"))
        return CountingStore(LocalStore(args.store, read_only=True))

    if args.compare_readers:
        _compare_readers(args, open_store)
        return

    import holoviews as hv

    hv.extension("bokeh")

    with open_store() as store:
        group = zarr.open_group(store, path=args.group, mode="r")
        for name, array in sorted(group.arrays()):
            print(
                f"{name}: shape={array.shape}, chunks={array.chunks}, shards={array.shards}, "
                f"dtype={array.dtype}, logical_bytes={array.nbytes}"
            )

    rows = []
    open_times = []
    time_path = "/".join(filter(None, (args.group, args.time)))
    for window in ("history", "month", "day", "stride16"):
        first = []
        repeated = []
        for _ in range(args.repeats):
            with open_store() as store:
                with time_stage("benchmark.dataset_open") as timer:
                    ds = xr.open_zarr(store, group=args.group, consolidated=False)
                    open_times.append(timer.elapsed() * 1000)
                with ds:
                    index = ds.indexes[args.time]
                    if (
                        not isinstance(index, pd.DatetimeIndex)
                        or index.empty
                        or not index.is_monotonic_increasing
                    ):
                        parser.error(
                            "expected a nonempty, ascending datetime coordinate"
                        )
                    if len(open_times) == 1:
                        _describe_sampling(index)
                        print(
                            f"Time coordinate GETs and bytes at open: {store.chunk_reads(time_path)}"
                        )
                    for cache, samples in (("cold", first), ("warm", repeated)):
                        with measure_operation(
                            "timeseries.benchmark", window=window, cache=cache
                        ):
                            samples.append(_measure(ds, store, args, window, hv))
        for cache, samples in (("cold", first), ("warm", repeated)):
            rows.append((f"{window}/{cache}", *map(median, zip(*samples))))

    print(
        "window/cache     samples  GETs     bytes    read   convert  curves   model   ready (ms)"
    )
    for label, points, gets, size, read, convert, curves, model, ready in rows:
        print(
            f"{label:<16} {points:7.0f} {gets:5.0f} {size:9.0f} "
            f"{read:7.2f} {convert:8.2f} {curves:7.2f} {model:7.2f} {ready:7.2f}"
        )
    print(
        f"Median fresh dataset open, including default coordinate indexing: {median(open_times):.2f} ms"
    )


if __name__ == "__main__":
    main()
