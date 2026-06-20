from pathlib import Path

import pytest

from zf.cli import main

def test_help(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    # argparse uses exit code 0 for --help
    assert exc.value.code == 0

    out, err = capsys.readouterr()
    assert "Zarr-fuse command line tool" in out
    assert "cp        Copy all datasets" in out
    assert "list" in out
    assert "Inspect a zarr store" in out


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
