# PowerShell script to set environment variables and start the HoloViz dashboard



# Run dashboard
Push-Location (Join-Path $PSScriptRoot "..")
try {
	panel serve app.py --show
} finally {
	Pop-Location
}
