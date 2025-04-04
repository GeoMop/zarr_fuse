import yaml
import attrs
import numpy as np
from pathlib import Path
from typing import Optional, List, Dict, Any, Union, IO

reserved_keys = set(['ATTRS', 'COORDS', 'VARS'])


@attrs.define
class Variable:
    name: str
    unit: Optional[str] = None
    description: Optional[str] = None
    coords: Union[str, List[str]] = None
    df_col: Optional[str] = None

    def __attrs_post_init__(self):
        """Set df_cols to [name] if not explicitly provided."""
        if self.df_col is None:
            self.df_col = self.name
        if self.coords is None:
            self.coords = [self.name]
        if isinstance(self.coords, str):
            self.coords = [self.coords]

    @property
    def attrs(self):
        return dict(
            unit=self.unit,
            description=self.description,
            df_col=self.df_col
        )


def _coord_values_converter(values):
    if values is None:
        return np.array([])
    elif isinstance(values, int):
        size = values
        return np.arange(size)
    else:
        values = np.atleast_1d(values)
        assert values.ndim == 1
        return values

@attrs.define
class Coord:
    name: str
    description: Optional[str] = None
    composed: Dict[str, List[Any]] = None
    values: Optional[np.ndarray]  = None              # Fixed size, given coord values.
    chunk_size: Optional[int] = 1024    # Explicit chunk size, 1024 default. Equal to 'len(values)' for fixed size coord.

    def __init__(self, **dict):
        if 'composed' not in dict or len(dict['composed']) == 0 or dict['composed'] is None:
            dict['composed'] = [dict['name']]
        _values = dict.get('values', [])
        if isinstance(_values, int):
            _values = np.atleast_2d(np.arange(_values)).T
        elif isinstance(_values, list):
            if len(_values) == 0:
                _values = np.empty( (0, len(dict['composed'])) )
            else:
                a = np.asarray(_values)
                if a.ndim == 1:
                    # Convert a 1D array to a column vector.
                    _values= a[:, np.newaxis]
                _values = np.atleast_2d(_values)

        dict['values'] = _values

        self.name = dict['name']
        self.description = dict.get('description', None)
        self.composed = dict['composed']
        self.values = dict['values']
        self.chunk_size = dict.get('chunk_size', 1024)


    def is_composed(self):
        return len(self.composed) > 1

Attrs = Dict[str, Any]
ZarrNodeStruc = Dict[str, Union[Attrs, Dict[str, Variable], Dict[str, Coord], 'ZarrNodeStruc']]

def set_name(d, name):
    assert isinstance(d, dict), f"Expected a dictionary, got: {d}"
    d['name'] = name
    return d

def dict_deserialize(content: dict) -> dict:
    """
    Recursively deserializes a dictionary.
    Processes special keys:
      - ATTRS: kept as is
      - COORDS: converted into a list of Coord objects
      - VARS: converted into a list of Quantity objects
    Other keys are processed recursively.
    """
    result = {}
    #assert 'VARS' in content, ValueError("VARS key is required.")
    #assert 'COORDS' in content, ValueError("COORDS key is required.")
    result['ATTRS'] = content.get('ATTRS', {})
    vars = content.get('VARS', {})
    result['VARS'] = {k: Variable(**set_name(v, k)) for k, v in vars.items()}
    coords = content.get('COORDS', {})
    result['COORDS'] = {k: Coord(**set_name(c, k)) for k, c in coords.items()}

    # Add implicitely defined coords.
    implicit_coords = [
        coord
        for var in result.get('VARS', {}).values()
        for coord in var.coords
    ]
    for coord in set(implicit_coords):
        coord_obj = result['COORDS'].setdefault(coord, Coord(name=coord))
        if not coord_obj.is_composed():
            result['VARS'].setdefault(coord, Variable(coord))


    for key, value in content.items():
        if key not in ['ATTRS', 'COORDS', 'VARS']:
            result[key] = dict_deserialize(value) if isinstance(value, dict) else value
    return result

def deserialize(source: Union[IO, str, bytes, Path]) -> dict:
    """
    Deserialize YAML from a file path, stream, or bytes containing YAML content.

    Parameters:
      source:
        - If str or Path, it is treated as a file path (and must exist).
        - If bytes, it is treated as YAML content (decoded as UTF-8).
        - Otherwise, it is assumed to be a file-like stream.

    Returns:
      A dictionary resulting from parsing the YAML and processing it with dict_deserialize().

    Raises:
      ValueError if a string is provided that does not correspond to an existing file.
      TypeError for unsupported types.
    """

    if isinstance(source, Path):
        # Try to open the source as a file path.
        with Path(source).open("r", encoding="utf-8") as file:
            content = file.read()
    elif isinstance(source, str):
        # Assume it's a string containing YAML content.
        content = source
    elif isinstance(source, bytes):
        # Decode bytes using UTF-8.
        content = source.decode("utf-8")
    else:
        # Assume it's a file-like stream.
        try:
            content = source.read()
        except Exception as e:
            raise TypeError("Provided source is not a supported type (IO, str, bytes, or Path)") from e

    raw_dict = yaml.safe_load(content)
    return dict_deserialize(raw_dict)


def convert_value(obj):
    """
    Recursively convert an object for YAML serialization.

    - If obj is an instance of an attrs class, convert it to a dict using
      attrs.asdict with this function as the value_serializer.
    - If obj is a dict, list, or tuple, process its elements recursively.
    - For basic types (int, float, str, bool, None), return the value as is.
    - Otherwise, return the string representation of obj.
    """
    if attrs.has(obj):
        return attrs.asdict(obj, value_serializer=lambda inst, field, value: convert_value(value))
    elif isinstance(obj, dict):
        return {k: convert_value(v) for k, v in obj.items()}
    elif hasattr(obj, 'dtype'):
        return obj.tolist()
    elif isinstance(obj, (list, tuple)):
        return [convert_value(item) for item in obj]
    else:
        return obj

def serialize(hierarchy: dict, path: Union[str, Path]=None) -> str:
    """
    Serialize a hierarchy of dictionaries (and lists/tuples) with leaf values that
    may be instances of attrs classes to a YAML string.

    The conversion is performed by the merged convert_value function which uses a
    custom value serializer for attrs.asdict.
    """
    converted = convert_value(hierarchy)
    content = yaml.safe_dump(converted, sort_keys=False)
    if path is None:
        return content
    else:
        with Path(path).open("w", encoding="utf-8") as file:
            file.write(content)
    return content

# Example Usage:
# tree_structure = read_structure('structure.yaml')
# write_structure(tree_structure, 'output_structure.yaml')

# def build_xarray_tree(structure: dict, source_path: Path = None) -> xr.DataTree:
#     """
#     Recursively builds an xarray DataTree from the given structure.
#
#     For each node:
#       - Create an xarray.Dataset.
#       - Update global attributes from 'ATTRS' if available.
#       - Add coordinates from 'COORDS' (empty arrays).
#       - Add variables from 'VARS' as DataArrays with dummy data.
#
#     Child nodes (any keys not in ['ATTRS', 'COORDS', 'VARS'])
#     are recursively processed and attached to the DataTree.
#
#     Parameters:
#       structure (dict): The deserialized structure.
#       name (str): Name for the current DataTree node.
#
#     Returns:
#       xr.DataTree: The resulting DataTree.
#     """
#     ds = xr.Dataset()
#
#     # Set dataset attributes if provided.
#     if "ATTRS" in structure:
#         ds.attrs.update(structure["ATTRS"])
#
#     # Process coordinates.
#     for coord in structure.get("COORDS", []):
#         # Add coordinate as an empty array; in practice you may want to fill real data.
#         ds = ds.assign_coords({coord.name: []})
#
#     # Process variables.
#     for var in structure.get("VARS", []):
#         if getattr(var, "shape", None) and getattr(var, "coords", None):
#             # Create a dummy zeros array using the provided shape and assign dims.
#             data = np.zeros(var.shape)
#             ds[var.name] = xr.DataArray(data, dims=var.coords)
#         else:
#             # Fallback to a scalar zero if no shape/dims info is provided.
#             ds[var.name] = xr.DataArray(0)
#
#     # Recursively build children DataTree nodes.
#     children = {}
#     for key, value in structure.items():
#         if key not in ['ATTRS', 'COORDS', 'VARS']:
#             children[key] = build_xarray_tree(value, name=key)
#
#     # Create and return the DataTree node.
#     if source_path:
#         name = source_path.name
#     else:
#         name = "."
#     ds.attrs["source_path"] = str(source_path)
#     return xr.DataTree(ds, name=name, children=children)

# def read_storage(yaml_path: Path) -> xr.DataTree:
#     structure = read_structure(yaml_path)
#     return build_xarray_tree(structure, source_path=yaml_path)