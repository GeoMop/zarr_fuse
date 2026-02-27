# PowerShell script to set environment variables and start the FastAPI backend

$env:ZF_S3_ACCESS_KEY = ""
$env:ZF_S3_SECRET_KEY = ""
$env:ZF_S3_ENDPOINT_URL = ""

# Run backend
Push-Location (Join-Path $PSScriptRoot "..")
try {
	uvicorn api.main:app --reload --host 127.0.0.1 --port 8000
} finally {
	Pop-Location
}
