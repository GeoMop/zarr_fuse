set -x

# TODO: 
# use miniconda installer to have Jupyter Lab environment

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Name of the virtual environment folder
#VENV_DIR="$SCRIPT_DIR/../../venv"


notebook=${1:-zoom_plot}

# then register it under a friendly name, e.g. "myenv":
${SCRIPT_DIR}/venv/bin/python3 -m ipykernel install \
    --user \
    --name hlavo_surface \
    --display-name "Python (hlavo_surface)"


    
CONDA_PATH="/opt/miniconda"
source $CONDA_PATH/etc/profile.d/conda.sh
conda activate jupy-env
jupyter-lab notebooks/${notebook}.ipynb
