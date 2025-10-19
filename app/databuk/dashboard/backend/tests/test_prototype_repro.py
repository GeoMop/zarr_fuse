"""
Test to reproduce and debug the FsspecStore.get() missing 'prototype' error.

This test demonstrates the compatibility issue where zarr_fuse
calls FsspecStore.get() without the required 'prototype' parameter.

REPRO TEST COMMANDS:
1. Schema-based Test (Main Test):
   python -m pytest tests/test_prototype_repro.py::test_zarr_fuse_schema_prototype_error -v -s

2. Direct Python execution:
   python tests/test_prototype_repro.py
"""

import pytest
import os
from pathlib import Path
from dotenv import load_dotenv


def load_s3_credentials():
    """Load S3 credentials from .env file."""
    env_path = Path(__file__).parent.parent / '.env'
    
    if env_path.exists():
        load_dotenv(env_path)
        print(f"Loaded credentials from: {env_path}")
    else:
        print(f"No .env file found at: {env_path}")
    
    # Get credentials
    endpoint_url = os.getenv('S3_ENDPOINT_URL')
    access_key = os.getenv('S3_ACCESS_KEY')
    secret_key = os.getenv('S3_SECRET_KEY')
    bucket_name = os.getenv('S3_BUCKET_NAME')
    
    if not all([endpoint_url, access_key, secret_key, bucket_name]):
        pytest.skip("S3 credentials not available")
    
    return {
        'endpoint_url': endpoint_url,
        'access_key': access_key,
        'secret_key': secret_key,
        'bucket_name': bucket_name
    }


def setup_s3_environment(creds):
    """Set up S3 credentials in environment variables."""
    os.environ['S3_ENDPOINT_URL'] = creds['endpoint_url']
    os.environ['S3_ACCESS_KEY'] = creds['access_key']
    os.environ['S3_SECRET_KEY'] = creds['secret_key']


@pytest.mark.xfail(reason="FsspecStore.get() requires prototype parameter")
def test_zarr_fuse_schema_prototype_error():
    """Test schema-based zarr_fuse triggers prototype error."""
    creds = load_s3_credentials()
    setup_s3_environment(creds)
    
    try:
        import zarr_fuse
        
        schema_path = Path(__file__).parent.parent / 'schemas' / 'test_schema.yaml'
        if not schema_path.exists():
            pytest.skip(f"Schema file not found: {schema_path}")
        
        print(f"Testing schema-based zarr_fuse with: {schema_path}")
        
        # Load schema and open store
        schema = zarr_fuse.zarr_schema.deserialize(schema_path)
        print("Schema loaded successfully")
        
        # This should trigger the prototype error
        store = zarr_fuse.open_store(schema)
        assert False, "Expected prototype error but schema-based zarr_fuse.open_store() succeeded"
        
    except TypeError as e:
        error_msg = str(e)
        print(f"Caught TypeError: {error_msg}")
        
        # Print full traceback for debugging
        import traceback
        print("Full traceback:")
        traceback.print_exc()
        
        if "prototype" in error_msg.lower():
            print("Successfully reproduced the prototype error with schema-based approach!")
            pytest.xfail(f"Compatibility issue: {error_msg}")
        else:
            raise
            
    except Exception as e:
        print(f"Unexpected error: {type(e).__name__}: {e}")
        raise


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
