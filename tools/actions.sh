# Download bucket into ./surface_bucket
#./aws.sh service s3 sync s3://hlavo-release/surface.zarr ./surface_bucket


# Delete bucket
#aws.sh service s3 rb s3://hlavo-release/surface.zarr --force

# Create bucket
./aws.sh service s3 mb s3://hlavo-release
