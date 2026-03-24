import os
import boto3

access_key = ""
secret_key = ""
endpoint_url = ""

bucket_name = "app-databuk-test-service"
local_root = r"C:\Users\fatih\Documents\GitHub\zarr_fuse\dashboard\config\bukov_endpoint\tiles"
target_prefix = "test_tiles/"

s3 = boto3.client(
    "s3",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    endpoint_url=endpoint_url,
)

uploaded = 0

for root, dirs, files in os.walk(local_root):
    for file in files:
        if not file.lower().endswith(".png"):
            continue

        local_path = os.path.join(root, file)

        # relative path inside the local tiles folder, e.g. 0/0/0.png
        rel_path = os.path.relpath(local_path, local_root).replace("\\", "/")

        # final S3 key, e.g. test_tiles/0/0/0.png
        s3_key = target_prefix + rel_path

        s3.upload_file(
            local_path,
            bucket_name,
            s3_key,
            ExtraArgs={
                "ContentType": "image/png",
                "ContentDisposition": "inline",
            },
        )

        uploaded += 1
        print(f"Uploaded s3://{bucket_name}/{s3_key}")

print(f"\nDone. Uploaded {uploaded} PNG files.")