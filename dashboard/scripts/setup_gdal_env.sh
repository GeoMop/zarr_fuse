#!/usr/bin/env bash
# setup_gdal_env.sh — Sets up GDAL for build_overlay_tiles.py on Linux/macOS.
# Usage: bash setup_gdal_env.sh
#
# What this does:
#   1. Finds or installs Miniconda
#   2. Configures conda for conda-forge only (avoids Anaconda ToS wall)
#   3. Creates a 'gdal-test' conda environment with GDAL + Python deps
#   4. Prints next steps (activate env, run the build script)
#
# 'set -euo pipefail' aborts on any failing command.

set -euo pipefail

echo ""
echo "=== GDAL Environment Setup ==="
echo ""

# --- Step 1: Find Miniconda ---

echo "[1/6] Looking for Miniconda..."

CONDA_EXE=""
CONDA_ROOT=""

for candidate in \
    "$HOME/miniconda3" \
    "$HOME/miniconda-new" \
    "$HOME/anaconda3" \
    "$HOME/Miniconda3"; do
    exe="$candidate/bin/conda"
    if [ -x "$exe" ]; then
        CONDA_EXE="$exe"
        CONDA_ROOT="$candidate"
        break
    fi
done

# --- Step 2: Install Miniconda if not found ---

if [ -z "$CONDA_EXE" ]; then
    echo "  Miniconda not found. Downloading..."

    INSTALLER="/tmp/miniconda-installer.sh"

    curl -fsSL \
        "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh" \
        -o "$INSTALLER"

    echo "  Installing Miniconda (this may take a minute)..."

    bash "$INSTALLER" -b -p "$HOME/miniconda3"

    CONDA_EXE="$HOME/miniconda3/bin/conda"
    CONDA_ROOT="$HOME/miniconda3"

    if [ ! -x "$CONDA_EXE" ]; then
        echo "  ERROR: Miniconda installation failed."
        echo "  Install manually from: https://docs.anaconda.com/miniconda/install/"
        exit 1
    fi

    echo "  Miniconda installed to: $CONDA_ROOT"
else
    echo "  Found: $CONDA_ROOT"
fi

# --- Step 3: Configure conda channels (conda-forge only) ---

echo "[2/6] Configuring conda channels (conda-forge only)..."

# Removing the Anaconda 'defaults' channel eliminates the Terms of Service
# wall that otherwise blocks non-interactive installs.
"$CONDA_EXE" config --remove channels defaults 2>/dev/null || true
"$CONDA_EXE" config --add channels conda-forge
"$CONDA_EXE" config --set channel_priority strict

echo "  conda-forge-only channels configured"

# --- Step 4: Initialize conda ---

echo "[3/6] Initializing conda..."

eval "$("$CONDA_EXE" shell.bash hook)"

echo "  conda initialized"

# --- Step 5: Create gdal-test environment ---

echo "[4/6] Creating gdal-test environment..."

ENV_DIR="$CONDA_ROOT/envs/gdal-test"

if [ -d "$ENV_DIR" ]; then
    echo "  gdal-test environment already exists"
else
    "$CONDA_EXE" create --yes --name gdal-test python=3.11
    echo "  gdal-test environment created"
fi

# --- Step 6: Install GDAL ---

echo "[5/6] Installing GDAL..."

"$CONDA_EXE" install --yes --name gdal-test gdal
echo "  GDAL installed"

# --- Step 7: Install Python dependencies ---

echo "[6/6] Installing Python dependencies..."

"$CONDA_EXE" run --name gdal-test python -m pip install pyyaml python-dotenv boto3
echo "  pyyaml, python-dotenv, boto3 installed"

# --- Done ---

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Next steps:"
echo "  1. Run: conda activate gdal-test"
echo "  2. Run: python build_overlay_tiles.py"
echo ""