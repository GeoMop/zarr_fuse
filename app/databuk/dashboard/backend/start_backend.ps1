# PowerShell script to set environment variables and start backend
$env:S3_BUCKET_NAME = "your_bucket_name"
$env:ZF_S3_ACCESS_KEY = "your_access_key"
$env:ZF_S3_SECRET_KEY = "your_secret_key"

# Run backend
python run.py
