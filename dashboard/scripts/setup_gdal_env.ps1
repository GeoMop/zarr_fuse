# setup_gdal_env.ps1 — Sets up GDAL for build_overlay_tiles.py on Windows.
# Usage: powershell -ExecutionPolicy Bypass -File setup_gdal_env.ps1
#
# What this does:
#   1. Finds or installs Miniconda
#   2. Configures conda for conda-forge only (avoids Anaconda ToS wall)
#   3. Creates a 'gdal-test' conda environment with GDAL + Python deps
#   4. Prints next steps (close terminal, reopen, run the build script)
#
# Every native command is checked via $LASTEXITCODE so a failure aborts
# with the failing step instead of printing a false success.

$ErrorActionPreference = "Stop"

function Assert-Command {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Step
    )
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  ERROR: $Step failed (exit code $LASTEXITCODE)." -ForegroundColor Red
        exit 1
    }
}

Write-Host ""
Write-Host "=== GDAL Environment Setup ===" -ForegroundColor Cyan
Write-Host ""

# --- Step 1: Find Miniconda ---

Write-Host "[1/6] Looking for Miniconda..." -ForegroundColor Yellow

$condaExe = $null
$condaRoot = $null

$possibleRoots = @(
    "$env:USERPROFILE\miniconda3",
    "$env:USERPROFILE\miniconda3-new",
    "$env:USERPROFILE\anaconda3",
    "$env:USERPROFILE\Miniconda3"
)

foreach ($root in $possibleRoots) {
    $exe = Join-Path $root "Scripts\conda.exe"
    if (Test-Path $exe) {
        $condaExe = $exe
        $condaRoot = $root
        break
    }
}

# --- Step 2: Install Miniconda if not found ---

if (-not $condaExe) {
    Write-Host "  Miniconda not found. Downloading..." -ForegroundColor Yellow

    $installer = Join-Path $env:TEMP "Miniconda3-latest-Windows-x86_64.exe"

    try {
        Invoke-WebRequest `
            -Uri "https://repo.anaconda.com/miniconda/Miniconda3-latest-Windows-x86_64.exe" `
            -OutFile $installer `
            -UseBasicParsing
    }
    catch {
        Write-Host "  ERROR: Download failed. Check your internet connection." -ForegroundColor Red
        exit 1
    }

    Write-Host "  Installing Miniconda (this may take a minute)..." -ForegroundColor Yellow

    $installArgs = @(
        '/InstallationType=JustMe',
        '/AddToPath=1',
        '/RegisterPython=0',
        '/S'
    )

    $proc = Start-Process -FilePath $installer -Wait -PassThru -ArgumentList $installArgs
    if ($proc.ExitCode -ne 0) {
        Write-Host "  ERROR: Miniconda installer failed (exit code $($proc.ExitCode))." -ForegroundColor Red
        exit 1
    }

    # Re-scan for conda after install
    foreach ($root in $possibleRoots) {
        $exe = Join-Path $root "Scripts\conda.exe"
        if (Test-Path $exe) {
            $condaExe = $exe
            $condaRoot = $root
            break
        }
    }

    if (-not $condaExe) {
        Write-Host "  ERROR: Miniconda installation failed." -ForegroundColor Red
        Write-Host "  Install manually from: https://docs.anaconda.com/miniconda/install/" -ForegroundColor Red
        exit 1
    }

    Write-Host "  Miniconda installed to: $condaRoot" -ForegroundColor Green
}
else {
    Write-Host "  Found: $condaRoot" -ForegroundColor Green
}

# --- Step 3: Configure conda channels (conda-forge only) ---

Write-Host "[2/6] Configuring conda channels (conda-forge only)..." -ForegroundColor Yellow

# Removing the Anaconda 'defaults' channel eliminates the Terms of Service
# wall that otherwise blocks non-interactive installs. Conda reads config
# from several files; a conda-root .condarc (e.g. written by a pre-existing
# install) keeps 'defaults' in the effective pool even after the user
# .condarc was cleaned, so patch both files when present.
function Set-ForgeOnlyChannels {
    param(
        [Parameter(Mandatory = $true)]
        [string]$CondaExe,
        [Parameter(Mandatory = $true)]
        [string[]]$Files
    )
    foreach ($file in $Files) {
        # --remove errors out when 'defaults' is absent; tolerate that.
        # Redirection of native stderr with $ErrorActionPreference="Stop"
        # turns the expected message into a terminating NativeCommandError,
        # so relax the preference around the tolerant call only.
        $oldEa = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            & $CondaExe config --file $file --remove channels defaults 2>$null | Out-Null
        }
        catch {
            # nothing to remove
        }
        $ErrorActionPreference = $oldEa
        & $CondaExe config --file $file --add channels conda-forge
        Assert-Command "channel configuration ($file)"
    }
    & $CondaExe config --set channel_priority strict
    Assert-Command "channel priority configuration"
}

Set-ForgeOnlyChannels -CondaExe $condaExe -Files @(
    "$env:USERPROFILE\.condarc",
    (Join-Path $condaRoot ".condarc")
)

Write-Host "  conda-forge-only channels configured" -ForegroundColor Green

# --- Step 4: Initialize conda for PowerShell ---

Write-Host "[3/6] Initializing conda..." -ForegroundColor Yellow

& $condaExe init powershell
Assert-Command "conda initialization"
Assert-Command "conda initialization"

Write-Host "  conda added to PATH for new terminals" -ForegroundColor Green

# --- Step 5: Create gdal-test environment ---

Write-Host "[4/6] Creating gdal-test environment..." -ForegroundColor Yellow

$envDir = Join-Path $condaRoot "envs\gdal-test"

if (Test-Path $envDir) {
    Write-Host "  gdal-test environment already exists" -ForegroundColor Green
}
else {
    & $condaExe create --yes --name gdal-test python=3.11
    Assert-Command "environment creation"
    Write-Host "  gdal-test environment created" -ForegroundColor Green
}

# --- Step 6: Install GDAL ---

Write-Host "[5/6] Installing GDAL..." -ForegroundColor Yellow

& $condaExe install --yes --name gdal-test gdal
Assert-Command "GDAL installation"
Write-Host "  GDAL installed" -ForegroundColor Green

# --- Step 7: Install Python dependencies ---

Write-Host "[6/6] Installing Python dependencies..." -ForegroundColor Yellow

$envPython = Join-Path $condaRoot "envs\gdal-test\python.exe"
& $envPython -m pip install pyyaml python-dotenv boto3
Assert-Command "pip dependency installation"
Write-Host "  pyyaml, python-dotenv, boto3 installed" -ForegroundColor Green

# --- Done ---

Write-Host ""
Write-Host "=== Setup Complete ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor White
Write-Host "  1. Close this terminal" -ForegroundColor White
Write-Host "  2. Open a new terminal" -ForegroundColor White
Write-Host "  3. Run: conda activate gdal-test" -ForegroundColor Green
Write-Host "  4. Run: python build_overlay_tiles.py" -ForegroundColor Green
Write-Host ""