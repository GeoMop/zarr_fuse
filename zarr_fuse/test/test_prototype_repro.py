r"""
Test suite for S3 zarr store operations with zarr_fuse.

This module contains tests that validate zarr_fuse's ability to write and read
zarr data to/from S3 storage using zarr_fuse's native API.

Tests use zarr_fuse's recommended approach (zf.open_store() + node.update()),
following the same patterns as zarr_fuse's own test suite (see test_zarr_storage.py).

S3 CONFIGURATION:
- Test bucket: test-zarr-storage
- Write test store: s3://test-zarr-storage/test_prototype/write_read_test.zarr
- Read test store: s3://hlavo-release/dashboard-test/structure_tree.zarr
- Endpoint: https://s3.cl4.du.cesnet.cz

REPRO TEST COMMANDS:
1. Run write+read test (uses test-zarr-storage bucket):
   cd C:\Users\fatih\Documents\GitHub\zarr_fuse
   python -m pytest zarr_fuse/test/test_prototype_repro.py::test_read_s3_zarr_store_via_zarr_fuse_api -v -s

2. Run pure zarr write test (investigate metadata issue):
   python -m pytest zarr_fuse/test/test_prototype_repro.py::test_write_with_pure_zarr_read_with_zarr_fuse -v -s

3. Run existing store test (uses hlavo-release bucket):
   python -m pytest zarr_fuse/test/test_prototype_repro.py::test_read_existing_s3_store -v -s

4. Run all tests in this file:
   python -m pytest zarr_fuse/test/test_prototype_repro.py -v -s

5. Direct Python execution:
   python zarr_fuse/test/test_prototype_repro.py

"""

import os
from pathlib import Path
import warnings

import fsspec
import numpy as np
import pytest
import zarr
from zarr.errors import ZarrUserWarning

import zarr_fuse


TEST_S3_ENDPOINT_URL = "https://s3.cl4.du.cesnet.cz"
TEST_S3_BUCKET_NAME = "test-zarr-storage"
EXISTING_STORE_URL = "s3://hlavo-release/dashboard-test/structure_tree.zarr"

def get_first_data_node(node):
    """
    Returns the first child node with data variables, or the node itself if it has data.
    """
    if list(getattr(node.dataset, 'data_vars', [])):
        return node
    for child_name, child_node in node.items():
        if list(getattr(child_node.dataset, 'data_vars', [])):
            return child_node
    return node  # fallback: return node itself if no child has data


def load_s3_credentials(secret_getenv):
    """Load only the S3 secret pair required by the prototype tests."""
    return {
        'access_key': secret_getenv('ZF_S3_ACCESS_KEY'),
        'secret_key': secret_getenv('ZF_S3_SECRET_KEY'),
    }


def _prototype_schema(store_url):
    return {
        "ATTRS": {
            "STORE_URL": store_url,
            "S3_ENDPOINT_URL": TEST_S3_ENDPOINT_URL,
        },
        "VARS": {
            "temperature": {
                "unit": "C",
                "coords": ["time"],
            },
        },
        "COORDS": {
            "time": {
                "unit": {"tick": "s", "tz": "UTC"},
                "source_unit": {"tick": "s", "tz": "UTC"},
                "chunk_size": 16,
            },
        },
    }


def _s3_storage_options(creds, *, asynchronous):
    return {
        "key": creds["access_key"],
        "secret": creds["secret_key"],
        "endpoint_url": TEST_S3_ENDPOINT_URL,
        "asynchronous": asynchronous,
        "config_kwargs": {
            "request_checksum_calculation": "when_required",
            "response_checksum_validation": "when_required",
        },
    }


def _make_fsspec_store(creds, store_path):
    fs = fsspec.filesystem("s3", **_s3_storage_options(creds, asynchronous=True))
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=".*fs .* was not created with `asynchronous=True`.*",
            category=ZarrUserWarning,
        )
        store = zarr.storage.FsspecStore(fs, path=store_path)
    return store


def _cleanup_store(creds, store_path):
    fs = fsspec.filesystem("s3", **_s3_storage_options(creds, asynchronous=False))
    try:
        fs.rm(store_path, recursive=True)
    except FileNotFoundError:
        pass


def test_read_s3_zarr_store_via_zarr_fuse_api(secret_getenv):
    """Test writing and reading zarr data to/from S3 using zarr_fuse's native API.
    
    This test validates the complete S3 workflow using zarr_fuse's recommended approach:
    - Write: zf.open_store() + node.update(polars.DataFrame)
    - Read: zf.open_store() + node.dataset
    
    This follows the same pattern as zarr_fuse's own tests (test_zarr_storage.py),
    avoiding manual S3FileSystem creation and configuration issues.
    
    S3 Configuration:
    - Bucket: test-zarr-storage
    - Store: s3://test-zarr-storage/test_prototype/write_read_test.zarr
    - Endpoint: https://s3.cl4.du.cesnet.cz
    
    Workflow:
    1. Write test data to S3 using zarr_fuse API
    2. Read it back using zarr_fuse API (which uses xarray internally)
    3. Verify data integrity
    
    Expected: Test PASSES (data successfully written and read from S3)
    """
    # Setup S3 credentials
    creds = load_s3_credentials(secret_getenv)
    
    schema_path = Path(__file__).parent / 'inputs' / 'test_schema.yaml'
    if not schema_path.exists():
        pytest.skip(f"Schema file not found: {schema_path}")
    schema = zarr_fuse.zarr_schema.deserialize(schema_path)
    store_url = f"s3://{TEST_S3_BUCKET_NAME}/test_prototype/write_read_test.zarr"
    schema.ds.ATTRS['STORE_URL'] = store_url
    
    print(f"\n=== STEP 1: Writing to S3 using zarr_fuse API ===")
    print(f"Store URL: {store_url}")
    print("Using: zf.open_store() + node.update(polars.DataFrame)")
    
    # Remove old store if exists - zarr_fuse handles S3 cleanup automatically
    kwargs = {"S3_ENDPOINT_URL": TEST_S3_ENDPOINT_URL}
    zarr_fuse.remove_store(schema, **kwargs)
    print("✓ Old store removed (if existed)")
    
    # Create/open the store using zarr_fuse's native API
    print("\nOpening store for writing...")
    node = zarr_fuse.open_store(schema, **kwargs)
    print("✓ Store opened")
    
    # Write data using zarr_fuse (same pattern as test_zarr_storage.py)
    print("\nWriting data with zarr_fuse...")
    import polars as pl
    test_data = pl.DataFrame({
        "time": [1000, 1001, 1002],
        "temperature": [20.0, 21.0, 22.0]
    })
    node.update(test_data)
    print("✓ Data written to S3")
    
    # Step 2: Read the data back using zarr_fuse API
    print("\n=== STEP 2: Reading from S3 using zarr_fuse API ===")
    print("Goal: Successfully read the zarr store from S3")
    print("Using: zf.open_store() + node.dataset\n")
    
    # Re-open the store for reading
    kwargs = {"S3_ENDPOINT_URL": TEST_S3_ENDPOINT_URL}
    node = zarr_fuse.open_store(schema, **kwargs)
    print("✓ Store opened")
    
    # Read the dataset - zarr_fuse uses xarray internally, which triggers the bug
    print("Reading dataset...")
    dataset = node.dataset
    
    print(f"✓ Dataset successfully read!")
    print(f"  Variables: {list(dataset.data_vars)}")
    print(f"  Coordinates: {list(dataset.coords)}")
    
    # Verify data integrity
    if 'temperature' in dataset.data_vars:
        temp = dataset['temperature'].values
        print(f"  Temperature data: {temp}")
        # Compare with expected values from test_data
        expected_temp = test_data['temperature'].to_numpy()
        np.testing.assert_array_equal(temp, expected_temp)
        print("✓ Data integrity verified!")
    
    print("\n✓ Test PASSED - Successfully wrote to and read from S3 using zarr_fuse API!")


def test_write_with_pure_zarr_read_with_zarr_fuse(secret_getenv):
    """Test writing with pure zarr (no xarray) and reading with zarr_fuse.
    
    This test investigates what happens when data is written using pure zarr library
    (without xarray metadata like _ARRAY_DIMENSIONS) and then read using zarr_fuse
    which relies on xarray for reading.
    
    S3 Configuration:
    - Bucket: test-zarr-storage
    - Store: s3://test-zarr-storage/test_prototype/write_read_test.zarr
    - Endpoint: https://s3.cl4.du.cesnet.cz
    
    Workflow:
    1. Write data using pure zarr locally (no xarray metadata)
    2. Upload to S3
    3. Attempt to read it back using zarr_fuse
    4. See if zarr_fuse can handle data without _ARRAY_DIMENSIONS
    
    Purpose: Understand how zarr_fuse handles zarr stores without xarray metadata.
    """
    # Setup S3 credentials
    creds = load_s3_credentials(secret_getenv)
    
    store_url = f"s3://{TEST_S3_BUCKET_NAME}/test_prototype/write_read_test.zarr"
    schema = zarr_fuse.zarr_schema.deserialize(_prototype_schema(store_url))
    
    print(f"\n=== STEP 1: Writing with PURE ZARR locally (no xarray metadata) ===")
    print(f"Target S3 URL: {store_url}")
    print("Method: Direct zarr.open_group() + create_dataset() on local filesystem")
    
    # Write directly to S3 with pure zarr and fsspec
    print("\nWriting data with pure zarr directly to S3...")
    store_path = store_url.removeprefix("s3://")
    _cleanup_store(creds, store_path)
    store = _make_fsspec_store(creds, store_path)
    root = zarr.open_group(store=store, mode="w")
    root.attrs["__structure__"] = zarr_fuse.zarr_schema.serialize(schema)

    # Create arrays WITHOUT _ARRAY_DIMENSIONS metadata
    time_data = np.array([1000, 1001, 1002])
    root.create_array("time", data=time_data, dimension_names=("time",))
    temp_data = np.array([20.0, 21.0, 22.0])
    root.create_array("temperature", data=temp_data, dimension_names=("time",))

    print("✓ Data written to S3 with pure zarr (no _ARRAY_DIMENSIONS metadata)")
    print(f"  - time: {time_data}")
    print(f"  - temperature: {temp_data}")

    print("\nChecking metadata...")
    if "_ARRAY_DIMENSIONS" in root["temperature"].attrs:
        print("  ⚠️  _ARRAY_DIMENSIONS found (unexpected!)")
    else:
        print("  ✓ No _ARRAY_DIMENSIONS metadata (as expected for pure zarr)")
    
    # Step 3: Read with zarr_fuse
    print("\n=== STEP 3: Reading with zarr_fuse ===")
    print("Question: Can zarr_fuse read zarr data without xarray metadata?\n")
    
    kwargs = {"S3_ENDPOINT_URL": TEST_S3_ENDPOINT_URL}
    
    print("Opening store with zarr_fuse...")
    node = zarr_fuse.open_store(schema, **kwargs)
    print("✓ Store opened")
    
    print("\nAttempting to read dataset...")
    try:
        dataset = node.dataset
        
        print(f"✅ SUCCESS! Dataset read successfully!")
        print(f"  Variables: {list(dataset.data_vars)}")
        print(f"  Coordinates: {list(dataset.coords)}")
        print(f"  Dimensions: {dataset.dims}")
        
        # Show what we got
        if 'temperature' in dataset.data_vars:
            temp = dataset['temperature'].values
            print(f"\n  Temperature values: {temp}")
            print(f"  Expected values: {temp_data}")
            
            # Verify data
            np.testing.assert_array_equal(temp, temp_data)
            print("  ✓ Data matches!")
        
        print("\n✅ CONCLUSION: zarr_fuse CAN read pure zarr data without _ARRAY_DIMENSIONS!")
        print("   This means hlavo-release issue might be something else.")
        
    except TypeError as e:
        if "prototype" in str(e):
            print(f"❌ PROTOTYPE ERROR: {e}")
            print("\n❌ CONCLUSION: zarr_fuse CANNOT read pure zarr data without _ARRAY_DIMENSIONS")
            print("   Root cause: Missing _ARRAY_DIMENSIONS triggers NCZarr fallback")
            print("   NCZarr code path has prototype parameter bug")
            print("\n   This confirms hlavo-release was also written with pure zarr!")
            raise
        else:
            raise
    except Exception as e:
        print(f"❌ Unexpected error: {type(e).__name__}: {e}")
        raise


def test_read_existing_s3_store(secret_getenv):
    """Test reading an existing production zarr store from S3 (hlavo-release bucket).
    
    This test validates zarr_fuse's ability to read pre-existing zarr stores
    that were created and stored on S3. Uses the production hlavo-release bucket
    which contains real atmospheric data.
    
    S3 Configuration:
    - Bucket: hlavo-release
    - Store: s3://hlavo-release/dashboard-test/structure_tree.zarr
    - Endpoint: https://s3.cl4.du.cesnet.cz
    
    Expected: Test PASSES (successfully read production data from S3)
    
    Note: If this test fails with "TypeError: FsspecStore.get() missing 'prototype' argument",
    it indicates a bug in xarray's zarr backend that prevents accessing production S3 data.
    """
    # Setup S3 credentials for hlavo-release bucket
    creds = load_s3_credentials(secret_getenv)
    
    print(f"\n=== Reading existing store from hlavo-release bucket ===")
    
    # Load schema configuration for production store
    schema_path = Path(__file__).parent / 'inputs' / 'test_schema.yaml'
    if not schema_path.exists():
        pytest.skip(f"Schema file not found: {schema_path}")
    
    print(f"Loading schema from: {schema_path}")
    schema = zarr_fuse.zarr_schema.deserialize(schema_path)
    print(f"Source used: schema.yaml ({schema_path})")
    store_url = EXISTING_STORE_URL
    schema.ds.ATTRS['STORE_URL'] = store_url
    print(f"Store URL: {store_url}")
    print("\nGoal: Successfully read production data from S3\n")
    # Try to open and read the store using zarr_fuse API
    kwargs = {"S3_ENDPOINT_URL": TEST_S3_ENDPOINT_URL}
    print("Opening production zarr store...")
    node = zarr_fuse.open_store(schema, **kwargs)
    print("✓ Store opened")
    print("Data read: using metadata from Zarr store.")
    print("\nReading dataset...")
    dataset = node.dataset
    
    print(f"✓ Dataset successfully read!")
    print(f"  Variables: {list(dataset.data_vars)}")
    print(f"  Coordinates: {list(dataset.coords)}")
    print(f"  Dimensions: {dataset.dims}")
    
        # Print sample data for all child groups and all variables (generic)
    for group_name, child_node in node.children.items():
        try:
            ds = child_node.dataset
        except Exception as e:
            print(f"--- Could not read dataset for group '{group_name}': {e}")
            continue
        print(f"\n--- Sample data from group '{group_name}' ---")
        for var_name in list(ds.data_vars):
            var = ds[var_name]
            print(f"  {var_name}:")
            print(f"    Shape: {var.shape}")
            print(f"    Dtype: {var.dtype}")
            # Print up to 5 values
            if var.size > 0:
                vals = var.values.flatten()
                print(f"    Sample values: {vals[:5]}")
        print(f"--- End of sample data for group '{group_name}' ---\n")

    print("\n✓ Test PASSED - Successfully read existing S3 zarr store!")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
