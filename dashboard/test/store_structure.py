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

