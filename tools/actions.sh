SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd $SCRIPT_DIR

# Download bucket into ./surface_bucket
#./aws.sh service s3 sync s3://hlavo-release/surface.zarr ./surface_bucket_v3
#./aws.sh service s3 sync s3://bukov/ ./bukov_bucket_06_09

# Upload recursively
#./aws.sh service s3 cp bukov_2022.zarr s3://bukov/store_2024.zarr --recursive


# Delete bucket
#aws.sh service s3 rb s3://hlavo-release/surface.zarr --force
#./aws.sh personal s3 rb s3://test-zarr-storage --force

# Create bucket
#./aws.sh service s3 mb s3://hlavo-release
./aws.sh service s3 mb s3://test-zarr-storage

# List bucket permissions
#./aws.sh service s3api get-bucket-acl --bucket hlavo-release
#./aws.sh personal s3api get-bucket-acl --bucket test-zarr-storage

# List buckets accessible from profile
# 

# Remove all objects with given prefix
#./aws.sh service s3 rm s3://bukov/store-2024.zarr --recursive
#./aws.sh service s3 rm s3://bukov/store_latlon_2024.zarr --recursive
#./aws.sh service s3 rm s3://bukov/store_latlon_2024.zarr --recursive
#s3://bukov/temperature_monitoring.zarr --recursive
#./aws.sh personal s3 rm s3://test-zarr-storage --recursive


# Rename objects with given prefix
# are implemented as copy + delete

#./aws.sh service s3 cp s3://bukov/store_latlon_2024.zarr s3://bukov/temperature_monitoring.zarr --recursive
