import uuid
import warnings

import fsspec
import numpy as np
import zarr
from zarr.errors import ZarrUserWarning


def _s3_test_config(secret_getenv):
    return {
        "bucket_name": secret_getenv("ZF_S3_BUCKET_NAME", "test-zarr-storage"),
        "endpoint_url": secret_getenv("ZF_S3_ENDPOINT_URL", "https://s3.cl4.du.cesnet.cz"),
        "access_key": secret_getenv("ZF_S3_ACCESS_KEY"),
        "secret_key": secret_getenv("ZF_S3_SECRET_KEY"),
    }


def _s3_storage_options(config):
    return


def _make_store(config, path):
    options = {
        "key": config["access_key"],
        "secret": config["secret_key"],
        "endpoint_url": config["endpoint_url"],
        "asynchronous": True,
        "config_kwargs": {
            "request_checksum_calculation": "when_required",
            "response_checksum_validation": "when_required",
        },
    }
    fs = fsspec.filesystem("s3", **options)
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=".*fs .* was not created with `asynchronous=True`.*",
            category=ZarrUserWarning,
        )
        store = zarr.storage.FsspecStore(fs, path=path)
    return fs, store


def _cleanup_store(config, path):
    options = {
        "key": config["access_key"],
        "secret": config["secret_key"],
        "endpoint_url": config["endpoint_url"],
        "asynchronous": False,
        "config_kwargs": {
            "request_checksum_calculation": "when_required",
            "response_checksum_validation": "when_required",
        },
    }
    fs = fsspec.filesystem("s3", **options)
    fs.rm(path, recursive=True)


def test_fsspec_store_s3_roundtrip(secret_getenv):
    config = _s3_test_config(secret_getenv)
    store_path = (
        f"{config['bucket_name']}/compatibility/"
        f"test_zarr_s3_{uuid.uuid4().hex}.zarr"
    )
    fs, store = _make_store(config, store_path)

    try:
        root = zarr.open_group(store=store, mode="w")
        root.attrs["source"] = "pytest"
        root.create_array("data", data=np.array([1, 2, 3], dtype=np.int64))

        reopened = zarr.open_group(store=store, mode="r")
        np.testing.assert_array_equal(reopened["data"][:], np.array([1, 2, 3], dtype=np.int64))
        assert reopened.attrs["source"] == "pytest"
    finally:
        _cleanup_store(config, store_path)
