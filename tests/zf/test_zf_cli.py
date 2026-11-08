from pathlib import Path

import numpy as np
import pytest
import xarray as xr
import zarr

from zf.cli import main

def test_help(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    # argparse uses exit code 0 for --help
    assert exc.value.code == 0

    out, err = capsys.readouterr()
    assert "Zarr-fuse command line tool" in out
    assert "Copy all datasets" in out
    assert "list" in out
    assert "Inspect a zarr store" in out
    assert "fix-dims" in out


def test_cp():
    main([
        'cp',
        '--src.STORE_URL=zip://surface.zarr.zip',
        '--dst.STORE_URL=file://dest.zarr',
        '',
        'dst_schema.yaml'])


def test_list(capsys):
    store = Path(__file__).resolve().parents[2] / "tools" / "hlavo_testing_bucket" / "profiles.zarr"
    main([
        "list",
        f"--store.STORE_URL=file://{store}",
    ])

    out, err = capsys.readouterr()
    assert "store: profiles.zarr" in out
    assert "profiles.zarr/Uhelna" in out


def _store_with_short_array(path: Path) -> None:
    """
    Group `node` with the date_time coordinate of length 10, `full` of length 10
    and `short` of length 7, as left by a write interrupted between the arrays.
    `short` was 8 long once: its edge chunk still holds a value past the shape.
    """
    node = zarr.open_group(str(path), mode="w").create_group("node")
    date_time = node.create_array(
        "date_time", shape=(10,), chunks=(4,), dtype="int64", fill_value=0,
        dimension_names=["date_time"],
    )
    date_time[:] = np.arange(10)
    x = node.create_array("x", shape=(3,), chunks=(3,), dtype="int64", fill_value=0, dimension_names=["x"])
    x[:] = np.arange(3)
    full = node.create_array(
        "full", shape=(10, 3), chunks=(4, 3), dtype="float64", fill_value=np.nan,
        dimension_names=["date_time", "x"],
    )
    full[:] = 1.0
    short = node.create_array(
        "short", shape=(8, 3), chunks=(4, 3), dtype="float64", fill_value=np.nan,
        dimension_names=["date_time", "x"],
    )
    short[:] = 2.0
    short.resize((7, 3))


def test_fix_dims_dry_run_reports_only(tmp_path, capsys):
    store = tmp_path / "store.zarr"
    _store_with_short_array(store)

    assert main(["fix-dims", f"--store.STORE_URL=file://{store}"]) == 1

    out, err = capsys.readouterr()
    assert "node dim=date_time target=10" in out
    assert "extend short: 7 -> 10" in out
    assert zarr.open_group(str(store), mode="r")["node/short"].shape == (7, 3)


def test_fix_dims_apply_extends_with_fill_value(tmp_path):
    store = tmp_path / "store.zarr"
    _store_with_short_array(store)

    assert main(["fix-dims", f"--store.STORE_URL=file://{store}", "--apply"]) == 0

    short = zarr.open_group(str(store), mode="r")["node/short"]
    assert short.shape == (10, 3)
    assert (short[:7] == 2.0).all()
    assert np.isnan(short[7:]).all()
    ds = xr.open_zarr(store, group="node", consolidated=False)
    assert ds.sizes["date_time"] == 10
    assert main(["fix-dims", f"--store.STORE_URL=file://{store}"]) == 0


def test_fix_dims_never_shrinks(tmp_path, capsys):
    store = tmp_path / "store.zarr"
    _store_with_short_array(store)
    node = zarr.open_group(str(store), mode="r+")["node"]
    node.create_array(
        "long", shape=(12, 3), chunks=(4, 3), dtype="float64", fill_value=np.nan,
        dimension_names=["date_time", "x"],
    )[:] = 3.0

    assert main(["fix-dims", f"--store.STORE_URL=file://{store}", "--apply"]) == 1

    out, err = capsys.readouterr()
    assert "skip long: 12 > 10, shrinking would drop data" in out
    assert zarr.open_group(str(store), mode="r")["node/long"].shape == (12, 3)
    assert zarr.open_group(str(store), mode="r")["node/short"].shape == (10, 3)
