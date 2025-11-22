#!/bin/bash
#
# Usage:
# ./create_env.sh [--force] 
#
# Install conda environment according to the 'conda-requirements.yml'
# The yaml config provides: name, source channel (conda-forge), list of installed packages

set -x


SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

# Environment for running fittings Jupyter notebook.
# Uses Conda environment
CONDA_PATH="/opt/miniconda"


# Function to install Miniconda
install_miniconda() {
# Update package list and install prerequisites
    sudo apt update
    sudo apt install -y wget bzip2

    # Define Miniconda installer URL, download.
    MINICONDA_URL="https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh"
    INSTALLER=/tmp/${MINICONDA_URL##*/}
    wget $MINICONDA_URL -O $INSTALLER
    
    # Run the installer silently (-b) and specify custom installation path (-p)
    sudo bash $INSTALLER -b -p $CONDA_PATH
    
    # Ensure the installation directory is writable by the current user
    sudo chown -R $USER:$USER $CONDA_PATH
    
    # Initialize conda for bash shell
    $CONDA_PATH/bin/conda init bash
        
    # Refresh the shell
    source ~/.bashrc
    
    echo "Miniconda installed at $CONDA_PATH."
}

# Main script execution
# check if conda is installed
if command -v conda &> /dev/null; then
    echo "Skipping Miniconda installation."
else
    install_miniconda
fi

# Extract the name field from the YAML file
env_yaml="$SCRIPTPATH/conda-requirements.yml"
env_name=$(grep '^name:' "$env_yaml" | awk '{print $2}')

# be sure activate works
source $CONDA_PATH/etc/profile.d/conda.sh


if [ "--force" == "$1" ]
then
    conda env remove -n $env_name
    conda env create  -y --file $env_yaml 
else
    conda env update -y --file $env_yaml
fi
conda activate $env_name
ipython kernel install --name "$env_name" --user


#conda install -y fenics-dolfinx pyvista  numpy scipy matplotlib plotly pandas

# List existing environments
conda env list
