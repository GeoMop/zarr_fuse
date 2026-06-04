## Simplified start script: run Python entrypoint from repository root.
Push-Location (Join-Path $PSScriptRoot "..\..")
try {
    python -m dashboard.serve_dashboard
} finally {
    Pop-Location
}