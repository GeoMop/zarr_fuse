# Load .env file
$envFile = Join-Path $PSScriptRoot ".env"
Write-Host "Using env file: $envFile"

if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*#' -or $_ -match '^\s*$') {
            return
        }

        $key, $value = $_ -split '=', 2
        $key = $key.Trim()
        $value = $value.Trim().Trim('"').Trim("'")

        [System.Environment]::SetEnvironmentVariable($key, $value, "Process")
    }
} else {
    Write-Host ".env file not found"
}

# Backward-compatible mapping for zarr_fuse storage expectations
if ($env:ZF_S3_ACCESS_KEY -and -not $env:S3_ACCESS_KEY) {
    [System.Environment]::SetEnvironmentVariable("S3_ACCESS_KEY", $env:ZF_S3_ACCESS_KEY, "Process")
}

if ($env:ZF_S3_SECRET_KEY -and -not $env:S3_SECRET_KEY) {
    [System.Environment]::SetEnvironmentVariable("S3_SECRET_KEY", $env:ZF_S3_SECRET_KEY, "Process")
}

if ($env:ZF_S3_ENDPOINT_URL -and -not $env:S3_ENDPOINT_URL) {
    [System.Environment]::SetEnvironmentVariable("S3_ENDPOINT_URL", $env:ZF_S3_ENDPOINT_URL, "Process")
}

# If HV_DASHBOARD_ENDPOINT is not set, use the first endpoint defined in endpoints.yaml
if (-not $env:HV_DASHBOARD_ENDPOINT) {
    $endpointsFile = Join-Path (Join-Path $PSScriptRoot "..") "config\endpoints.yaml"
    Write-Host "Using endpoints file: $endpointsFile"

    if (Test-Path $endpointsFile) {
        $firstEndpoint = $null

        Get-Content $endpointsFile | ForEach-Object {
            if (-not $firstEndpoint -and $_ -match '^\s*([A-Za-z0-9_]+):\s*$') {
                $firstEndpoint = $matches[1]
            }
        }

        if ($firstEndpoint) {
            [System.Environment]::SetEnvironmentVariable("HV_DASHBOARD_ENDPOINT", $firstEndpoint, "Process")
        }
    }
}

Write-Host "HV_DASHBOARD_ENDPOINT=$env:HV_DASHBOARD_ENDPOINT"
Write-Host "S3_ACCESS_KEY set:" ([bool]$env:S3_ACCESS_KEY)
Write-Host "S3_SECRET_KEY set:" ([bool]$env:S3_SECRET_KEY)
Write-Host "S3_ENDPOINT_URL=$env:S3_ENDPOINT_URL"

# Run dashboard
Push-Location (Join-Path $PSScriptRoot "..")
try {
    python serve_dashboard.py
} finally {
    Pop-Location
}