# Download bucket into ./surface_bucket
#./aws.sh service s3 sync s3://hlavo-release/surface.zarr ./surface_bucket_v3
#./aws.sh service s3 sync s3://bukov/ ./bukov_bucket_v0

# Upload recursively
./aws.sh service s3 cp bukov_2022.zarr s3://bukov/store_2024.zarr --recursive


# Delete bucket
#aws.sh service s3 rb s3://hlavo-release/surface.zarr --force

# Create bucket
#./aws.sh service s3 mb s3://hlavo-release

# List bucket permissions
#./aws.sh service s3api get-bucket-acl --bucket hlavo-release

# List buckets accessible from profile
# 

# Remove all objects with given prefix
#./aws.sh service s3 rm s3://bukov/store-2024.zarr --recursive
