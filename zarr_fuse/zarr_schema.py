import yaml
import attrs
import numpy as np
from pathlib import Path
from typing import Optional, List, Dict, Any, Union, IO, Tuple

from . import units

reserved_keys = set(['ATTRS', 'COORDS', 'VARS'])


def _unit_converter(unit):
    """
    Convert unit to a string.
    """
    if unit is None:
        return None
    elif isinstance(unit, str):
        return unit
    elif isinstance(unit, dict):
        return units.DateTimeUnit(**unit)
    else:
        raise TypeError(f"Unsupported unit type: {type(unit)}")

@attrs.define
class Variable:
    name: str
    unit: Optional[str] = attrs.field(default='', converter=_unit_converter)    # dimensionless
    description: Optional[str] = None
    coords: Union[str, List[str]] = None
    df_col: Optional[str] = None
    source_unit: Optional[str] = attrs.field(default=None, converter=_unit_converter)

    def __attrs_post_init__(self):
        """Set df_cols to [name] if not explicitly provided."""
        if self.df_col is None:
            self.df_col = self.name
        if self.source_unit is None:
            self.source_unit = self.unit
        if self.coords is None:
            self.coords = [self.name]
        if isinstance(self.coords, str):
            self.coords = [self.coords]

    @property
    def attrs(self):
        return dict(
            unit=self.unit,
            description=self.description,
            df_col=self.df_col,
            source_unit = self.source_unit
        )

@attrs.define(slots=False)
class Coord:
    name: str
    description: Optional[str] = None
    composed: Dict[str, List[Any]] = None
    sorted: bool = True
    chunk_size: Optional[int] = 1024    # Explicit chunk size, 1024 default. Equal to 'len(values)' for fixed size coord.
    step_limits: Optional[List[float]] = []    # [min_step, max_step, unit]
    #values: Optional[np.ndarray]  = attrs.field(eq=attrs.cmp_using(eq=np.array_equal), default=None)             # Fixed size, given coord values.

    def __init__(self, **dict):
        if 'composed' not in dict or len(dict['composed']) == 0 or dict['composed'] is None:
            dict['composed'] = [dict['name']]
        # _values = dict.get('values', [])
        # if isinstance(_values, int):
        #     _values = np.atleast_2d(np.arange(_values)).T
        # elif isinstance(_values, list):
        #     if len(_values) == 0:
        #         _values = np.empty( (0, len(dict['composed'])) )
        #     else:
        #         a = np.asarray(_values)
        #         if a.ndim == 1:
        #             # Convert a 1D array to a column vector.
        #             _values= a[:, np.newaxis]
        #         _values = np.atleast_2d(_values)
        #
        # dict['values'] = _values

        self.name = dict['name']
        self.description = dict.get('description', None)
        self.composed = dict['composed']
        #self.values = dict['values']
        self.chunk_size = dict.get('chunk_size', 1024)
        self.step_limits = dict.get('step_limits', [])
        if self.step_limits is not None and len(self.step_limits) > 0:
            if len(self.step_limits) == 1:
                self.step_limits = 2 * self.step_limits
            if len(self.step_limits) == 2:
                self.step_limits = [*self.step_limits, '']
            assert len(self.step_limits) == 3, f"step_limits should be a list of 3 elements, got {self.step_limits}"
            assert isinstance(self.step_limits[2], str)

        self.sorted = dict.get('sorted', not self.is_composed())
        # May be explicit in the future. Namely, if we support interpolation of sparse coordinates.
        self._variables = None # set in DatasetSchema.__init__

    @property
    def attrs(self):
        """
        Coordinate attributes set as attribute of rthe esulting Dataset variable.
        Does not affect serialization.
        :return: dict of exported attributes
        """
        return dict(
            composed=self.composed,
            description=f"\n\n{self.description}",
            chunk_size=self.chunk_size,
        )

    def is_composed(self):
        return len(self.composed) > 1

    def step_unit(self):
        """
        Return the unit of the step_limits.
        """
        coord_unit = self._variables[self.name].unit
        step_unit = units.step_unit(coord_unit)
        return step_unit


Attrs = Dict[str, Any]

attrs_field = attrs.field
# Overcome the name conflict within DatasetSchema class.
@attrs.define
class DatasetSchema:
    ATTRS: Attrs = attrs_field(factory=dict)
    COORDS: Dict[str, Coord] = attrs_field(factory=dict)
    VARS: Dict[str, Variable] = attrs_field(factory=dict)


    def __init__(self, attrs, vars, coords):
        self.ATTRS = attrs
        self.VARS = DatasetSchema.safe_instance(Variable, vars)
        self.COORDS = DatasetSchema.safe_instance(Coord, coords)

        # Add implicitely defined coords.
        implicit_coords = [
            coord
            for var in self.VARS.values()
            for coord in var.coords
        ]
        for coord in set(implicit_coords):
            coord_obj = self.COORDS.setdefault(coord, Coord(name=coord))
            if not coord_obj.is_composed():
                self.VARS.setdefault(coord, Variable(coord))

        # Link coords to variables
        for coord in self.COORDS.values():
            coord._variables = self.VARS

    @staticmethod
    def safe_instance(cls, kwargs_dict):
        """
        Create an instance of the given class with the provided keyword arguments.
        If the class is not defined, return None.
        """

        def set_name(d, name):
            assert isinstance(d, dict), f"Expected a dictionary, got: {d}"
            d['name'] = name
            return d

        vars = {k: cls(**set_name(v, k)) for k, v in kwargs_dict.items()}
        return vars

    def is_empty(self):
        """
        Check if the schema is empty.
        """
        return not self.ATTRS and not self.COORDS and not self.VARS

@attrs.define
class NodeSchema:
    ds: DatasetSchema
    groups: Dict[str, 'NodeSchema'] = attrs.field(factory=dict)


def dict_deserialize(content: dict) -> NodeSchema:
    """
    Recursively deserializes the schema = tree of node dictionaries.
    Create instances of DatasetScheme from special keys:
      - ATTRS: kept as is
      - COORDS: converted into a list of Coord objects
      - VARS: converted into a list of Quantity objects
    Other keys are processed recursively.

    TODO: report path to error key
    """
    ds_schema = DatasetSchema(
        attrs = content.pop('ATTRS', {}),
        vars=content.pop('VARS', {}),
        coords = content.pop('COORDS', {})
    )

    children = {
        key: dict_deserialize(value)
        for key, value in content.items()
    }

        # if isinstance(value, dict) else value
    return NodeSchema(ds_schema, children)

def deserialize(source: Union[IO, str, bytes, Path]) -> NodeSchema:
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
    return  dict_deserialize(raw_dict)

def convert_value(obj: NodeSchema):
    """
    Recursively convert an object for YAML serialization.

    - If obj is an instance of an attrs class, convert it to a dict using
      attrs.asdict with this function as the value_serializer.
    - If obj is a dict, list, or tuple, process its elements recursively.
    - For basic types (int, float, str, bool, None), return the value as is.
    - Otherwise, return the string representation of obj.
    """
    if isinstance(obj, NodeSchema):
        children_dict = convert_value(obj.groups)
        assert set(children_dict.keys()).isdisjoint(reserved_keys)

        ds_dict = convert_value(obj.ds)
        children_dict.update(ds_dict)
        return children_dict

    if attrs.has(obj):
        # Serialize attrs instances, serialize values recursively.
        return attrs.asdict(obj,
                            value_serializer=
                            lambda inst, field, value: convert_value(value))
    elif isinstance(obj, dict):
        return {k: convert_value(v) for k, v in obj.items()}
    elif hasattr(obj, 'dtype'):
        return obj.tolist()
    elif isinstance(obj, (list, tuple)):
        return [convert_value(item) for item in obj]
    else:
        return obj

def serialize(node_schema: NodeSchema, path: Union[str, Path]=None) -> str:
    """
    Serialize a hierarchy of dictionaries (and lists/tuples) with leaf values that
    may be instances of attrs classes to a YAML string.

    The conversion is performed by the merged convert_value function which uses a
    custom value serializer for attrs.asdict.
    """
    converted = convert_value(node_schema)
    content = yaml.safe_dump(converted, sort_keys=False)
    if path is None:
        return content
    else:
        with Path(path).open("w", encoding="utf-8") as file:
            file.write(content)
    return content

