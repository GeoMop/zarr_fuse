#!/usr/bin/env python3
"""
S3 Test Script for Dashboard
Creates and manages Zarr stores in CESNET S3 bucket using common S3 configuration
"""

import os
import sys
import zarr
import s3fs
import fsspec
import numpy as np
import traceback
from pathlib import Path
from dotenv import load_dotenv

# Import common S3 configuration from backend
sys.path.append('backend')
from services.s3_service import get_s3_storage_options

# Load environment variables
load_dotenv()

class S3ZarrTester:
    def __init__(self):
        self.bucket_name = os.getenv("S3_BUCKET_NAME", "test-zarr-storage")
        self.endpoint_url = os.getenv("S3_ENDPOINT_URL", "https://s3.cl4.du.cesnet.cz")
        self.access_key = os.getenv("S3_ACCESS_KEY")
        self.secret_key = os.getenv("S3_SECRET_KEY")
        
        if not all([self.access_key, self.secret_key]):
            raise ValueError("S3_ACCESS_KEY and S3_SECRET_KEY must be set in .env file")
        
        self.fs = None
        self.connect()
    
    def connect(self):
        """Connect to S3 using common S3 configuration"""
        # Use fsspec directly like backend
        storage_options = dict(
            key=self.access_key,
            secret=self.secret_key,
            client_kwargs=dict(endpoint_url=self.endpoint_url),
            config_kwargs={
                "s3": {
                    "payload_signing_enabled": False,
                    "addressing_style": "path"
                },
                "retries": {"max_attempts": 5, "mode": "standard"},
                "connect_timeout": 20,
                "read_timeout": 60,
                "request_checksum_calculation": "when_required",
                "response_checksum_validation": "when_required",
            },
        )
        
        self.fs = fsspec.filesystem('s3', **storage_options)
        print(f"✅ Connected to S3: {self.endpoint_url}")
    
    def create_test_tree(self, store_path="dashboard-test/structure_tree.zarr"):
        """Create a test Zarr tree structure"""
        print(f"🌳 Creating test tree at: s3://{self.bucket_name}/{store_path}")
        
        # Use fsspec.get_mapper with zarr_fuse storage_options pattern
        storage_options = dict(
            key=self.access_key,
            secret=self.secret_key,
            client_kwargs=dict(endpoint_url=self.endpoint_url),
            config_kwargs={
                "s3": {
                    "payload_signing_enabled": False,
                    "addressing_style": "path"
                },
                "retries": {"max_attempts": 5, "mode": "standard"},
                "connect_timeout": 20,
                "read_timeout": 60,
                "request_checksum_calculation": "when_required",
                "response_checksum_validation": "when_required",
            },
        )
        
        print(f"🔧 Using storage options: {storage_options}")
        store = fsspec.get_mapper(f"s3://{self.bucket_name}/{store_path}", **storage_options)
        print(f"✅ Created mapper for: s3://{self.bucket_name}/{store_path}")
        
        # Create root group
        root = zarr.open_group(store, mode='w')
        print(f"✅ Created root group")
        
        # Quick low-level write test to validate S3 put
        try:
            test_key = f"{self.bucket_name}/dashboard-test/.write_check.tmp"
            with self.fs.open(test_key, 'wb') as fh:
                fh.write(b'hello')
            print(f"✅ Low-level write OK: s3://{test_key}")
            self.fs.rm(test_key)
        except Exception as e:
            print(f"❌ Low-level write FAILED: {e}")
            traceback.print_exc()

        # Add root data (with detailed error reporting)
        try:
            print(f"📄 Creating temperature array...")
            arr = zarr.open_array(store, path='temperature', mode='w', shape=(3,), chunks=(3,), dtype=np.float64, compressor=None)
            arr[:] = np.array([280.0, 281.0, 282.0], dtype=np.float64)
            print(f"✅ Created temperature array")
        except Exception as e:
            print(f"❌ Failed to create 'temperature' array: {e}")
            traceback.print_exc()
            raise

        try:
            print(f"📄 Creating time array...")
            arr = zarr.open_array(store, path='time', mode='w', shape=(3,), chunks=(3,), dtype=np.int64, compressor=None)
            arr[:] = np.array([1000, 1001, 1002], dtype=np.int64)
            print(f"✅ Created time array")
        except Exception as e:
            print(f"❌ Failed to create 'time' array: {e}")
            traceback.print_exc()
            raise
        
        # Create child_1 group
        print(f"📁 Creating child_1 group...")
        child1 = root.create_group('child_1')
        try:
            arr = zarr.open_array(store, path='child_1/temperature', mode='w', shape=(2,), chunks=(2,), dtype=np.float64, compressor=None)
            arr[:] = np.array([283.0, 284.0], dtype=np.float64)
            arr = zarr.open_array(store, path='child_1/time', mode='w', shape=(2,), chunks=(2,), dtype=np.int64, compressor=None)
            arr[:] = np.array([1003, 1004], dtype=np.int64)
        except Exception as e:
            print(f"❌ Failed to create arrays under 'child_1': {e}")
            traceback.print_exc()
            raise
        
        # Create child_1/child_3 group
        print(f"📁 Creating child_3 group...")
        child3 = child1.create_group('child_3')
        try:
            arr = zarr.open_array(store, path='child_1/child_3/temperature', mode='w', shape=(1,), chunks=(1,), dtype=np.float64, compressor=None)
            arr[:] = np.array([285.0], dtype=np.float64)
            arr = zarr.open_array(store, path='child_1/child_3/time', mode='w', shape=(1,), chunks=(1,), dtype=np.int64, compressor=None)
            arr[:] = np.array([1005], dtype=np.int64)
        except Exception as e:
            print(f"❌ Failed to create arrays under 'child_3': {e}")
            traceback.print_exc()
            raise
        
        # Create child_2 group
        print(f"📁 Creating child_2 group...")
        child2 = root.create_group('child_2')
        try:
            arr = zarr.open_array(store, path='child_2/temperature', mode='w', shape=(3,), chunks=(3,), dtype=np.float64, compressor=None)
            arr[:] = np.array([286.0, 287.0, 288.0], dtype=np.float64)
            arr = zarr.open_array(store, path='child_2/time', mode='w', shape=(3,), chunks=(3,), dtype=np.int64, compressor=None)
            arr[:] = np.array([1006, 1007, 1008], dtype=np.int64)
        except Exception as e:
            print(f"❌ Failed to create arrays under 'child_2': {e}")
            traceback.print_exc()
            raise
        
        # Add metadata
        root.attrs['description'] = 'Test tree structure for dashboard'
        root.attrs['created_by'] = 'dashboard-test-script'
        print(f"✅ Added metadata")
        
        # Debug: verify root is a group and print attributes
        try:
            test_root = zarr.open_group(store, mode='r')
            print(f"🔎 Root opened as group successfully: {isinstance(test_root, zarr.hierarchy.Group)}")
            print(f"🔎 Root attrs: {dict(test_root.attrs)}")
        except Exception as e:
            print(f"❌ Could not open root as group: {e}")

        # Debug: check Zarr marker files at root (v2 .zgroup or v3 zarr.json)
        try:
            marker_v2 = store.get('.zgroup', None) is not None
            marker_v3 = store.get('zarr.json', None) is not None
            print(f"🔎 Marker files - .zgroup: {marker_v2}, zarr.json: {marker_v3}")
        except Exception as e:
            print(f"⚠️  Could not check marker files: {e}")

        # Debug: recursively count groups and arrays
        def _count_nodes(group) -> tuple[int, int]:
            num_groups = 1  # count this group
            num_arrays = 0
            for name in group.keys():
                node = group[name]
                if hasattr(node, 'shape'):
                    num_arrays += 1
                else:
                    g, a = _count_nodes(node)
                    num_groups += g
                    num_arrays += a
            return num_groups, num_arrays

        try:
            g_count, a_count = _count_nodes(root)
            print(f"📊 Node counts -> groups: {g_count}, arrays: {a_count}")
        except Exception as e:
            print(f"⚠️  Could not count nodes: {e}")

        # Debug: list S3 paths directly (top-level and one level deep)
        try:
            base = f"{self.bucket_name}/{store_path}"
            top = self.fs.ls(base, detail=True)
            print(f"📁 Top-level in s3://{base} ({len(top)} items):")
            for t in top:
                print(f"  - {t['name']} ({t['type']})")
            # List immediate subdirectories
            for t in top:
                if t['type'] == 'directory':
                    sub = self.fs.ls(t['name'], detail=True)
                    print(f"  📂 {t['name']} ({len(sub)} items):")
                    for s in sub[:20]:  # limit spam
                        print(f"    - {s['name']} ({s['type']})")
        except Exception as e:
            print(f"⚠️  Could not list S3 paths for verification: {e}")

        print(f"✅ Test tree created successfully!")
        print(f"📊 Root group keys: {list(root.keys())}")
        return store_path
    
    def list_bucket_contents(self):
        """List all contents in the bucket"""
        print(f"📁 Bucket contents: s3://{self.bucket_name}")
        try:
            files = self.fs.ls(self.bucket_name, detail=True)
            for file in files:
                print(f"  {'📁' if file['type'] == 'directory' else '📄'} {file['name']}")
                
                # If it's a directory, list its contents too
                if file['type'] == 'directory':
                    try:
                        sub_files = self.fs.ls(file['name'], detail=True)
                        for sub_file in sub_files:
                            print(f"    {'📁' if sub_file['type'] == 'directory' else '📄'} {sub_file['name']}")
                    except Exception as e:
                        print(f"    ❌ Error listing subdirectory: {e}")
        except Exception as e:
            print(f"❌ Error listing bucket: {e}")
    
    def read_test_tree(self, store_path="dashboard-test/structure_tree.zarr"):
        """Read and display the test tree structure using zarr_fuse pattern"""
        print(f"📖 Reading test tree from: s3://{self.bucket_name}/{store_path}")
        
        try:
            # Use fsspec.get_mapper directly like in create_test_tree
            storage_options = dict(
                key=self.access_key,
                secret=self.secret_key,
                client_kwargs=dict(endpoint_url=self.endpoint_url),
                config_kwargs={
                    "s3": {
                        "payload_signing_enabled": False,
                        "addressing_style": "path"
                    },
                    "retries": {"max_attempts": 5, "mode": "standard"},
                    "connect_timeout": 20,
                    "read_timeout": 60,
                    "request_checksum_calculation": "when_required",
                    "response_checksum_validation": "when_required",
                },
            )
            
            store = fsspec.get_mapper(f"s3://{self.bucket_name}/{store_path}", **storage_options)
            root = zarr.open_group(store, mode='r')
            
            print("📊 Tree structure:")
            self._print_group_structure(root, level=0)
            
        except Exception as e:
            print(f"❌ Error reading tree: {e}")
            traceback.print_exc()
    
    def _print_group_structure(self, group, level=0):
        """Recursively print group structure"""
        indent = "  " * level
        
        # Print datasets and groups
        for name in group.keys():
            dataset = group[name]
            if hasattr(dataset, 'shape'):
                print(f"{indent}📄 {name}: shape={dataset.shape}, dtype={dataset.dtype}")
            else:
                print(f"{indent}📁 {name}/")
                self._print_group_structure(dataset, level + 1)
    
    def cleanup_bucket(self):
        """Remove all test data from bucket"""
        print(f"🧹 Cleaning up bucket: s3://{self.bucket_name}")
        
        try:
            # List all files
            files = self.fs.ls(self.bucket_name, detail=True)
            
            # Remove test directories
            for file in files:
                if file['type'] == 'directory' and ('dashboard-test' in file['name'] or 'test' in file['name']):
                    print(f"🗑️  Removing: {file['name']}")
                    self.fs.rm(file['name'], recursive=True)
            
            print("✅ Bucket cleanup completed!")
            
        except Exception as e:
            print(f"❌ Error during cleanup: {e}")

def main():
    """Main function"""
    print("🚀 S3 Zarr Test Script for Dashboard")
    print("=" * 50)
    
    try:
        tester = S3ZarrTester()
        
        # Show current bucket contents
        print("\n📋 Current bucket contents:")
        tester.list_bucket_contents()
        
        # Create test tree
        print("\n🔨 Creating test tree...")
        store_path = tester.create_test_tree()
        
        # Read and verify
        print("\n📖 Verifying created tree...")
        tester.read_test_tree(store_path)
        
        print("\n✅ Test completed successfully!")
        print(f"📝 Test data available at: s3://{tester.bucket_name}/{store_path}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="S3 Zarr Test Script")
    parser.add_argument("--cleanup", action="store_true", help="Clean up test data from bucket")
    parser.add_argument("--list", action="store_true", help="List bucket contents")
    parser.add_argument("--read", type=str, help="Read specific store path")
    
    args = parser.parse_args()
    
    try:
        tester = S3ZarrTester()
        
        if args.cleanup:
            tester.cleanup_bucket()
        elif args.list:
            tester.list_bucket_contents()
        elif args.read:
            tester.read_test_tree(args.read)
        else:
            main()
            
    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        sys.exit(1)
