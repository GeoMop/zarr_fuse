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


def test_cp():
    main([
        'cp',
        '--src.STORE_URL=zip://surface.zarr.zip',
        '--dst.STORE_URL=file://dest.zarr',
        '',
        'dst_schema.yaml'])