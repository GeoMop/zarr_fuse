# Should be tested and fully integrated to the dashboard, but this is the general structure for the tile building configuration. The actual implementation of tile building and serving would require additional code to generate tiles from the source image and serve them via a web server or similar mechanism.
import boto3

endpoint_url = ""
access_key = ""
secret_key = ""

import boto3

bucket_name = "app-databuk-test-service"
prefix = "test_tiles/"

s3 = boto3.client(
    "s3",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    endpoint_url=endpoint_url,
)

resp = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
for obj in resp.get("Contents", []):
    print(obj["Key"])

