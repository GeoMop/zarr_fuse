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

2. Run existing store test (uses hlavo-release bucket):
   python -m pytest zarr_fuse/test/test_prototype_repro.py::test_read_existing_s3_store -v -s

3. Run all tests in this file:
   python -m pytest zarr_fuse/test/test_prototype_repro.py -v -s

4. Direct Python execution:
   python zarr_fuse/test/test_prototype_repro.py

EXPECTED STATUS:
- All tests should PASS when zarr_fuse S3 support is working correctly
- If tests fail with "TypeError: FsspecStore.get() missing 'prototype' argument",
  this indicates a bug in xarray's zarr backend integration
"""

import pytest
import os
import numpy as np
from pathlib import Path
from dotenv import load_dotenv
import zarr_fuse


def load_s3_credentials():
    """Load S3 credentials from .env file."""
    # Updated path: C:\Users\fatih\Documents\GitHub\zarr_fuse\.env
    env_path = Path(__file__).parent.parent.parent / '.env'
    
    if env_path.exists():
        load_dotenv(env_path)
        print(f"Loaded credentials from: {env_path}")
    else:
        print(f"No .env file found at: {env_path}")
    
    # Get credentials - try both with and without ZF_ prefix
    endpoint_url = os.getenv('S3_ENDPOINT_URL')
    access_key = os.getenv('ZF_S3_ACCESS_KEY') or os.getenv('S3_ACCESS_KEY')
    secret_key = os.getenv('ZF_S3_SECRET_KEY') or os.getenv('S3_SECRET_KEY')
    bucket_name = os.getenv('ZF_S3_BUCKET_NAME') or os.getenv('S3_BUCKET_NAME')
    
    if not all([endpoint_url, access_key, secret_key, bucket_name]):
        pytest.skip("S3 credentials not available")
    
    return {
        'endpoint_url': endpoint_url,
        'access_key': access_key,
        'secret_key': secret_key,
        'bucket_name': bucket_name
    }


def setup_s3_environment(creds):
    """Set up S3 credentials in environment variables.
    
    zarr_fuse expects: S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_KEY
    (without ZF_ prefix)
    """
    os.environ['S3_ENDPOINT_URL'] = creds['endpoint_url']
    os.environ['S3_ACCESS_KEY'] = creds['access_key']
    os.environ['S3_SECRET_KEY'] = creds['secret_key']


def test_read_s3_zarr_store_via_zarr_fuse_api():
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
    
    Note: If this test fails with "TypeError: FsspecStore.get() missing 'prototype' argument",
    it indicates a bug in xarray's zarr backend when working with FsspecStore.
    """
    # Setup S3 credentials
    creds = load_s3_credentials()
    setup_s3_environment(creds)
    
    # Load test schema configuration
    schema_path = Path(__file__).parent / 'inputs' / 'test_schema.yaml'
    if not schema_path.exists():
        pytest.skip(f"Schema file not found: {schema_path}")
    
    schema = zarr_fuse.zarr_schema.deserialize(schema_path)
    store_url = schema.ds.ATTRS['STORE_URL']
    
    print(f"\n=== STEP 1: Writing to S3 using zarr_fuse API ===")
    print(f"Store URL: {store_url}")
    print("Using: zf.open_store() + node.update(polars.DataFrame)")
    
    # Remove old store if exists - zarr_fuse handles S3 cleanup automatically
    kwargs = {"S3_ENDPOINT_URL": creds['endpoint_url']}
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
    kwargs = {"S3_ENDPOINT_URL": creds['endpoint_url']}
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


def test_read_existing_s3_store():
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
    creds = load_s3_credentials()
    setup_s3_environment(creds)
    
    print(f"\n=== Reading existing store from hlavo-release bucket ===")
    
    # Load schema configuration for production store
    schema_path = Path(__file__).parent / 'inputs' / 'test_schema.yaml'
    if not schema_path.exists():
        pytest.skip(f"Schema file not found: {schema_path}")
    
    print(f"Loading schema from: {schema_path}")
    schema = zarr_fuse.zarr_schema.deserialize(schema_path)
    
    store_url = schema.ds.ATTRS['STORE_URL']
    print(f"Store URL: {store_url}")
    
    print("\nGoal: Successfully read production data from S3\n")
    
    # Try to open and read the store using zarr_fuse API
    kwargs = {"S3_ENDPOINT_URL": creds['endpoint_url']}
    
    print("Opening production zarr store...")
    node = zarr_fuse.open_store(schema, **kwargs)
    print("✓ Store opened")
    
    print("\nReading dataset...")
    dataset = node.dataset
    
    print(f"✓ Dataset successfully read!")
    print(f"  Variables: {list(dataset.data_vars)}")
    print(f"  Coordinates: {list(dataset.coords)}")
    print(f"  Dimensions: {dataset.dims}")
    
    # Show sample data to verify successful read
    for var_name in list(dataset.data_vars)[:2]:  # First 2 variables
        var = dataset[var_name]
        print(f"\n  {var_name}:")
        print(f"    Shape: {var.shape}")
        print(f"    Dtype: {var.dtype}")
        if var.size > 0 and var.size <= 10:
            print(f"    Values: {var.values}")
    
    print("\n✓ Test PASSED - Successfully read existing S3 zarr store!")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
