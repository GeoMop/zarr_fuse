# PowerShell script to set environment variables and start the HoloViz dashboard


$env:ZF_S3_ACCESS_KEY = ""
$env:ZF_S3_SECRET_KEY = ""
$env:ZF_S3_ENDPOINT_URL = ""


# Run dashboard
Push-Location (Join-Path $PSScriptRoot "..")
try {
	panel serve app.py --show
} finally {
	Pop-Location
}
