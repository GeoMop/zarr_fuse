#!/bin/bash
# This would create a development virtual environment
# - uses requirements.txt
# - install endorse itself in development mode.
set -x

echo "Creating python virtual environment."

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 || exit ; pwd -P )"
SRC_ROOT="$SCRIPTPATH"

cd ${SRC_ROOT} || exit
rm -r venv
#virtualenv venv
python3 -m venv --system-site-packages venv
ls

venv_pip=${SRC_ROOT}/venv/bin/pip
$venv_pip install wheel
$venv_pip install --upgrade pip
#source venv/bin/activate
$venv_pip install "Flask-SQLAlchemy>=3.0.1"
#$venv_pip install -r requirements.txt

# TODO simplyfy dependency and submodules
# attrs somehow was broken after gmsh explicit installation, must force its reinstalation

$venv_pip install -e .
