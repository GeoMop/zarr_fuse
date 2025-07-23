import pytest
import zarr
import s3fs
import os
import uuid
from zarr_fuse.zarr_storage import open_storage
from zarr_fuse.zarr_schema import NodeSchema, DatasetSchema

# Read S3 configuration from environment variables
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")

# Condition to skip the test if S3 environment variables are not set
s3_configured = all([S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET_NAME])

@pytest.mark.skipif(not s3_configured, reason="S3 integration test environment variables not set")
def test_s3_integration():
    """
    Integration test that connects to a real S3-compatible service (like MinIO)
    to verify the creation, existence, and deletion of a Zarr store.
    """
    zarr_path = f"integration_test_{uuid.uuid4().hex}.zarr"
    store_url = f"s3://{S3_BUCKET_NAME}/{zarr_path}"

    storage_options = {
        "key": S3_ACCESS_KEY,
        "secret": S3_SECRET_KEY,
        "client_kwargs": {
            "endpoint_url": S3_ENDPOINT_URL,
        },
    }

    s3_schema = NodeSchema(
        ds=DatasetSchema(
            {
                "store_url": store_url,
                "store_type": "remote",
                "storage_options": storage_options,
            },
            {},  # VARS
            {}   # COORDS
        ),
        groups={}
    )

    fs = s3fs.S3FileSystem(**storage_options)

    # Ensure the path is clean before we start
    if fs.exists(store_url):
        fs.rm(store_url, recursive=True)

    try:
        # 1. Attempt to open (and create) the storage
        root_node = open_storage(s3_schema)
        assert root_node is not None
        assert isinstance(root_node.store, zarr.storage.FsspecStore)

        # 2. Verify we can see the created .zgroup file
        assert fs.exists(f"{store_url}/.zgroup")

    finally:
        # 3. Clean up by removing the created Zarr store
        if fs.exists(store_url):
            fs.rm(store_url, recursive=True)
            assert not fs.exists(store_url) 