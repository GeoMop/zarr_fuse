import os

import numpy as np
import polars as pl
import pytest

import zarr_fuse as zf
from zarr_fuse.logger import StoreLogHandler


def _schema(store_url) -> dict:
    return {
        "ATTRS": {"STORE_URL": str(store_url)},
        "VARS": {"temp": {"unit": "", "type": "float64", "coords": ["t"]}},
        "COORDS": {"t": {"unit": "", "type": "float64"}},
        "meteo": {
            "VARS": {"rain": {"unit": "", "type": "float64", "coords": ["t"]}},
            "COORDS": {"t": {"unit": "", "type": "float64"}},
        },
    }


def test_commit_publishes_the_session(tmp_path):
    """Writes of the session become visible on the branch only by the commit of the root node."""
    repo = zf.open_repository(_schema(tmp_path / "repo"))
    root = repo.open(mode="a")
    root.update(pl.DataFrame({"t": [0.0, 1.0], "temp": [10.0, 11.0]}))
    root["meteo"].update(pl.DataFrame({"t": [0.0], "rain": [1.0]}))
    assert len(list(repo.ic.ancestry(branch="main"))) == 1

    new_root = root.commit("first import")

    assert root.mode == "r"
    assert root["meteo"].mode == "r"
    assert new_root.mode == "a"
    main = repo.open()
    np.testing.assert_array_equal(main.dataset["temp"].values, [10.0, 11.0])
    np.testing.assert_array_equal(main["meteo"].dataset["rain"].values, [1.0])


def test_node_at_reads_other_states(tmp_path):
    """The same node at a snapshot, a tag or a branch shows the data of that state, read-only."""
    repo = zf.open_repository(_schema(tmp_path / "repo"))
    root = repo.open(mode="a")
    root["meteo"].update(pl.DataFrame({"t": [0.0], "rain": [1.0]}))
    root = root.commit("first")
    first = repo.ic.lookup_branch("main")
    root["meteo"].update(pl.DataFrame({"t": [1.0], "rain": [2.0]}))
    root.commit("second")
    repo.ic.create_tag("release-1", first)
    repo.ic.create_branch("dev", first)

    meteo = repo.open()["meteo"]

    np.testing.assert_array_equal(meteo.dataset["rain"].values, [1.0, 2.0])
    for old in (meteo.at(snapshot=first), meteo.at(tag="release-1"), meteo.at("dev")):
        assert old.mode == "r"
        np.testing.assert_array_equal(old.dataset["rain"].values, [1.0])


def test_readonly_open_keeps_the_stored_tree(tmp_path):
    """A read-only open neither creates nor fails on the nodes added to the schema later."""
    schema = _schema(tmp_path / "repo")
    zf.open_repository(schema).open(mode="a").commit("init")
    schema["extra"] = {
        "VARS": {"x": {"unit": "", "type": "float64", "coords": ["t"]}},
        "COORDS": {"t": {"unit": "", "type": "float64"}},
    }

    root = zf.open_repository(schema).open()

    assert set(root.children) == {"meteo"}


def test_only_the_root_commits(tmp_path):
    """Children share the session of the root, so only the root can commit it."""
    root = zf.open_repository(_schema(tmp_path / "repo")).open(mode="a")

    with pytest.raises(ValueError):
        root["meteo"].commit("child")


def test_writable_repository_node_does_not_log_to_the_store(tmp_path):
    """IceChunk store accepts only zarr keys, the log records can not be written into it."""
    root = zf.open_repository(_schema(tmp_path / "repo")).open(mode="a")

    assert not any(isinstance(handler, StoreLogHandler) for handler in root.logger.handlers)


def test_repository_on_s3(load_repo_secret_env):
    """Commit and read back a repository in the test S3 bucket."""
    if "ZF_S3_ACCESS_KEY" not in os.environ:
        pytest.skip("S3 credentials ZF_S3_ACCESS_KEY and ZF_S3_SECRET_KEY are not set.")
    tox_env_name = os.environ.get("TOX_ENV_NAME", "local")
    schema = _schema(f"s3://test-zarr-storage/{tox_env_name}/repository_tst.icechunk")
    options = {"S3_ENDPOINT_URL": "https://s3.cl4.du.cesnet.cz"}
    zf.remove_store(schema, **options)

    root = zf.open_repository(schema, **options).open(mode="a")
    root["meteo"].update(pl.DataFrame({"t": [0.0, 1.0], "rain": [1.0, 2.0]}))
    root.commit("s3 import")

    meteo = zf.open_repository(schema, **options).open()["meteo"]
    np.testing.assert_array_equal(meteo.dataset["rain"].values, [1.0, 2.0])
