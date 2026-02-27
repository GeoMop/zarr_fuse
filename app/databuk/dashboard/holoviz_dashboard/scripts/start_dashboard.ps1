# PowerShell script to set environment variables and start the HoloViz dashboard

$env:ZF_S3_ACCESS_KEY = "your_access_key"
$env:ZF_S3_SECRET_KEY = "your_secret_key"
$env:ZF_S3_ENDPOINT_URL = ""

$env:HV_DASHBOARD_ENDPOINT = ""

# Run dashboard
panel serve app.py --show
