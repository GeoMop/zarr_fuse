from pint import UnitRegistry, set_application_registry
ureg = UnitRegistry()
set_application_registry(ureg)

# Use of relative imports is recomenden  in __init__.py
from . import zarr_schema as schema
from . import units
from .zarr_storage import Node, open_store

try:
    from . import plot
except ModuleNotFoundError as e:
    if hasattr(e, '__optional_dependency__'):
        pass
    else:
        raise e

# What is allowed to be imported by
# from zarr_fuse import *
__all__ = ['schema', 'Node', 'open_store']