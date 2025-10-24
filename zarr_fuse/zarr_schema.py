from typing import *

import pandas
import yaml
import attrs
import numpy as np
import warnings
from pathlib import Path

from . import units
from . import logger as zf_logger

"""
Classes to represent the store schema. It enables:
- serialization / deserialization with stable deserialized state
- recursive comparison of schemas to test for schema changes

Deserialization has to deal with possible errors in the schema file / dictionary.
Deserialization and all underlying functions accepts optional log parameter, 
this is use preferably as a logger, otherwise a default logger is used.

TODO:
- move abstract serialization/deserialization and context wrapper to a separate module; keep here just schema classes / functions
- Catch more errors, raise only on essential errors, leading to invalid schema
- Ability to turn error logs to raise (dry run mode)
"""


class RaisingLogger(zf_logger.Logger):
    def error(self, exc, *args, **kwargs):
        super().error(exc, *args, **kwargs)
        if not isinstance(exc, BaseException):
            exc = RuntimeError(str(exc))
        raise exc # fallback if a plain message was passed


def default_logger():
    return RaisingLogger("default schema logger")


class SchemaErrBase:
    """
    Mixin that holds message and its origincontext for both
    the Exception and the Warning classes.
    """
    def __init__(self, message: str, ctx: 'SchemaCtx'):
        self.message = message
        self.address = ctx

    def __str__(self) -> str:
        return f"{self.message}  (at {self.address})"


class SchemaError(SchemaErrBase, Exception):
    """Raise when the config problem should be fatal."""
    pass


class SchemaWarning(SchemaErrBase, UserWarning):
    """Emit when the problem should be non-fatal."""
    pass







SchemaKey = Union[str, int]
SchemaPath = SchemaKey | List[SchemaKey]
@attrs.define(frozen=True)
class SchemaCtx:
    """
    Represents a single value in the schema file.
    Holds the source file name (or empty string for an anonymous stream)
    and a list of path components locating the value within the YAML tree.

    Path components are stored as provided (str or int) and converted to
    strings only when rendering.
    """
    addr: SchemaPath
    file: str = attrs.field(default=None, eq=False)
    logger: zf_logger.Logger = attrs.field(factory=default_logger)

    @property
    def path(self) -> str:
        """Return the path as a string."""
        return self._join(self.addr)

    @staticmethod
    def _join(addr):
        return '/'.join(map(str, addr))

    def __str__(self) -> str:
        file_repr = self.file if self.file else "<SCHEMA STREAM>"
        return f"{file_repr}:{self.path}"

    def dive(self, *path) -> "SchemaCtx":
        """Return a new SchemaAddress with an extra path component."""
        addr = self.addr + list(path)
        return SchemaCtx(addr, self.file, self.logger)

    def error(self, message: str, **kwargs) -> None:
        err = SchemaError(message, self)
        self.logger.error(err, **kwargs)
        return err

    def warning(self, message: str, **kwargs) -> None:
        warn = SchemaWarning(message, self)
        self.logger.warning(warn, **kwargs)
        return warn

@attrs.define
class ContextCfg:
    cfg: Dict[str, Any] | List[Any]
    schema_ctx: SchemaCtx

    def __getitem__(self, key: str| int) -> Any:
        return ContextCfg(self.cfg[key], self.schema_ctx.dive(key))

    def __setitem__(self, key: str| int, value: Any) -> None:
        self.cfg[key] = value

    def __contains__(self, item):
        return item in self.cfg

    def value(self) -> Any:
        return self.cfg

    def pop(self, key: str| int, default=None) -> Any:
        value = self.cfg.pop(key, default)
        return ContextCfg(value, self.schema_ctx.dive(key))

    def keys(self):
        return self.cfg.keys()

    def get(self, key: str| int, default=None) -> Any:
        value = self.cfg.get(key, default)
        return ContextCfg(value, self.schema_ctx.dive(key))



class AddressMixin:
    """
    Mixin for schema objects that keeps their source SchemaAddress and
    provides convenience helpers to raise errors / emit warnings bound
    to that address.

    The helpers accept an optional list of `subkeys` to append to the
    stored address. This is useful when the message refers to a nested
    attribute/key (e.g., a field inside ATTRS/VARS/COORDS).
    """
    _address: SchemaCtx  # subclasses provide this via attrs field

    def _extend_address(self, subkeys: List[Union[str, int]] = []) -> SchemaCtx:
        addr = self._address
        for k in subkeys:
            addr = addr.dive(k)
        return addr

    def error(self, message: str, subkeys: Optional[List[Union[str, int]]] = []) -> None:
        # Deprecated: use self._address.error() directly
        addr = self._extend_address(subkeys)
        addr.error(message)

    def warn(self, message: str, subkeys: Optional[List[Union[str, int]]] = [], *, stacklevel: int = 2) -> None:
        # deprecated: use self._address.warning() directly
        addr = self._extend_address(subkeys)
        addr.warning(message, stacklevel=stacklevel,)


# ----------------------- Converters & core classes ----------------------- #


  # # Enforce unit constraint: when unit is provided, bool and str[n] are not allowed.
  #   if unit is not None and target_dtype is not None:
  #       if target_dtype == np.dtype(bool):
  #           raise ValueError("Boolean dtype is not allowed when 'unit' is provided.")
  #       if str_len is not None:
  #           raise ValueError("Fixed-length string dtype 'str[n]' is not allowed when 'unit' is provided.")

Scalar = bool | int | float | str
Unit = str | units.DateTimeUnit
RangeTuple = None | List[None] | Tuple[Scalar, Scalar] | Tuple[Scalar,Scalar, Unit]

def unit_instance(cfg: ContextCfg) -> Unit:
    """
    Create instance of pint.Unit for a string intput or
    instance of DateTimeUnit for the dict input.."""
    unit, schema_ctx = cfg.value(), cfg.schema_ctx
    if unit is None:
        return None
    elif isinstance(unit, str):
        try:
            return units.Unit(unit)
        except Exception as e:
            cfg.schema_ctx.error(f"Invalid unit string: {unit}. Pint error: {e}")
    elif isinstance(unit, dict):
        try:
            return units.DateTimeUnit(**unit)
        except Exception as e:
            cfg.schema_ctx.error(f"Invalid DateTimeUnit dict: {unit}. Error: {e}")
    else:
        cfg.schema_ctx.error(f"Unsupported unit: {unit}")
        pass
    return ""


@attrs.define
class DiscreteRange(AddressMixin):
    values: List[Any] | str

    @staticmethod
    def from_cfg(cfg: ContextCfg, source_col: str, convert_fn):
        if isinstance(cfg.value(), str):
            # read values from a CSV file
            # TODO: for large discrete value arrays we should store them as a zarr array not in attrs
            path = Path(cfg.schema_ctx.file) / cfg.value()
            df = pandas.read_csv(path)
            values = convert_fn(df[source_col])
        else:
            values = np.array(cfg.value())
        return DiscreteRange(values)



    def asdict(self, value_serializer, filter):
        return {'discrete': list(self.values)}

@attrs.define
class Interval(AddressMixin):
    """
    Start and end could be None to represent open intervals.
    """
    start: Any
    end: Any
    unit: Unit

    @classmethod
    def from_list(cls, cfg: ContextCfg, default_unit):
        lst, schema_ctx = cfg.value(), cfg.schema_ctx
        if not isinstance(lst, list):
            lst = [lst]
        if len(lst) == 0:
            lst = [None, None]
        if len(lst) == 1:
            lst = 2 * lst

        if len(lst) == 2:
            return cls(lst[0], lst[1], default_unit)
        elif len(lst) == 3:
            unit = unit_instance(cfg[2])
            return cls(lst[0], lst[1], unit)
        else:
            cfg.schema_ctx.error(f"Invalid interval specification: {lst}")

    def asdict(self, value_serializer, filter):
        return [self.start, self.end, value_serializer(self, 2, self.unit)]

@attrs.define
class IntervalRange(Interval):
    def asdict(self, value_serializer, filter):
        return {'interval': Interval.asdict(self, value_serializer, filter)}


@attrs.define
class Variable(AddressMixin):

    def __init__(self, dict:ContextCfg):
        self._address = dict.schema_ctx
        assert self._address is not None

        # Obligatory attributes
        self.name: str = dict["name"].value()
        self.coords: Union[str, List[str]] = dict.get("coords").value()
        if isinstance(self.coords, str):
            self.coords = [self.coords]

        # Optional attributes
        self.type: Optional[str] = dict.get("type", None).value()

        unit_item = dict.get("unit", "")
        self.unit: Optional[Unit] = unit_instance(unit_item)

        self.description: Optional[str] = dict.get("description", None).value()
        self.df_col: Optional[str] = dict.get("df_col", self.name).value()

        source_unit_item = dict.get("source_unit", unit_item.value())
        self.source_unit: Optional[Unit] = unit_instance(source_unit_item)

        # Depends on unit, df_col, source_unit
        self.range: DiscreteRange | IntervalRange \
            = self.range_instance(dict.get("range", None))

    @property
    def attrs(self):
        return dict(
            unit=self.unit,
            description=self.description,
            df_col=self.df_col,
            source_unit=self.source_unit,
        )

    def asdict(self, value_serializer, filter):
        """
        Custom asdict to override coords serialization.
        """
        return {
            k: value_serializer(self, k, v)
            for k, v in self.__dict__.items()
            if filter(k, v)
            }

    def range_instance(self, dict_ctx: ContextCfg):
        range_dict, schema_ctx = dict_ctx.value(), dict_ctx.schema_ctx
        if range_dict is None:
            return IntervalRange.from_list(ContextCfg([None, None], schema_ctx), self.unit)

        if 'discrete' in range_dict:
            return DiscreteRange.from_cfg(dict_ctx['discrete'], self.df_col, self.convert_values)
        elif 'interval' in range_dict:
            return IntervalRange.from_list(dict_ctx['interval'], self.unit)

        schema_ctx.error(f"Invalid range specification: {range_dict}")

    def convert_values(self, values):
        """
        Convert from source units to the variable's unit, check type
        :param values:
        :return:
        """
        return units.create_quantity(values, unit=self.source_unit, type=self.type).to(self.unit)

class Coord(Variable):

    def __init__(self, cfg:ContextCfg):
        # Default coords list is  only for coords.
        if 'coords' not in cfg:
            cfg['coords'] = cfg['name'].value()
        super().__init__(cfg)

        composed = cfg.get("composed", None).value()
        if composed is None or len(composed) == 0:
            composed= [self.name]
        self.composed: Dict[str, List[Any]] = composed

        # Explicit chunk size, 1024 default. Equal to 'len(values)' for fixed size coord.
        self.chunk_size : Optional[int] \
            = cfg.get("chunk_size", 1024).value()

        step_item = cfg.get("step_limits", [])
        if step_item.value() is  None:
            self.step_limits = None
        else:
            default_step_unit = units.step_unit(self.unit) # For DateTimeUnit the incremental/step unit is a pint.Unit of time.
            self.step_limits = Interval.from_list(step_item, default_step_unit)

        self.sorted : bool = cfg.get('sorted', not self.is_composed()).value()

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
        coord_unit = self.unit
        step_unit = units.step_unit(coord_unit)
        return step_unit


Attrs = Dict[str, Any]
attrs_field = attrs.field
# Overcome the name conflict within DatasetSchema class.

reserved_keys = set(["ATTRS", "COORDS", "VARS"])

@attrs.define
class DatasetSchema(AddressMixin):
    _address: SchemaCtx = attrs.field(alias="_address", repr=False, eq=False)
    ATTRS: Attrs = attrs_field(factory=dict)
    COORDS: Dict[str, Coord] = attrs_field(factory=dict)
    VARS: Dict[str, Variable] = attrs_field(factory=dict)

    def __init__(self, _address: SchemaCtx, attrs: ContextCfg,
                 vars: ContextCfg, coords: ContextCfg):
        self._address = _address
        self.ATTRS = attrs.value()

        # Backward compatible definition of coords as VARS.
        for coord in coords.keys():
            if coord in vars:
                self.warn(
                    f"OBSOLETE. Coordinate '{coord}' is defined both in VARS and COORDS. ",
                    subkeys=["COORDS", coord],
                )
                coords[coord].value().update(vars[coord])
                del vars.value()[coord]

        self.VARS = self.safe_instance(Variable, vars)
        # Add implicitly defined coords.
        for v in self.VARS.values():
            assert isinstance(v.coords, list), f"var: {v.name}"
        implicit_coords = [coord for var in self.VARS.values() for coord in var.coords]
        for coord in set(implicit_coords):
            coords.value().setdefault(
                coord, dict(name=coord)
            )

        self.COORDS = self.safe_instance(Coord, coords)


        # Link coords to variables
        # TODO: Getting rid of default coord variable injection, we could now move this to Coord constructor.
        for coord in self.COORDS.values():
            coord._variables = self.VARS

    def safe_instance(self, cls, kwargs_dict: ContextCfg):
        """
        Create mapping of name -> instance for the given class with the provided keyword arguments.
        Ensures that each instance receives its origin SchemaAddress based on self._address.
        """
        out: Dict[str, Any] = {}
        for name in kwargs_dict.keys():
            d = kwargs_dict[name]
            if  not isinstance(d.value(), dict):
                d.schema_ctx.error(f"Expected a dictionary, got: {type(d).__name__}")
            d["name"] = name
            out[name] = cls(d)
        return out

    def is_empty(self):
        return not self.ATTRS and not self.COORDS and not self.VARS


@attrs.define
class NodeSchema(AddressMixin):
    _address: SchemaCtx = attrs.field(alias="_address", repr=False, eq=False)
    ds: DatasetSchema
    groups: Dict[str, "NodeSchema"] = attrs.field(factory=dict)

    def asdict(self, value_serializer, filter):
        """
        Custom asdict to override ds and groups serialization.
        """
        children_dict = value_serializer(self, "", self.groups)
        assert set(children_dict.keys()).isdisjoint(reserved_keys)

        ds_dict = value_serializer(self, "", self.ds)
        children_dict.update(ds_dict)
        return children_dict

# ----------------------- (De)serialization helpers ----------------------- #

def dict_deserialize(content: ContextCfg) -> NodeSchema:
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
        _address=content.schema_ctx,
        attrs = content.pop('ATTRS', {}),
        vars=content.pop('VARS', {}),
        coords = content.pop('COORDS', {})
    )

    children = {
        key: dict_deserialize(content[key])
        for key in content.keys()
    }

    return NodeSchema(_address=content.schema_ctx, ds=ds_schema, groups=children)


def deserialize(source: Union[IO, str, bytes, Path],
                source_description=None, log: zf_logger.Logger=None) -> NodeSchema:
    """
    Deserialize YAML from a file path, stream, or bytes containing YAML content.

    Parameters:
      source:
        - If str or Path, it is treated as a file path (and must exist).
        - If bytes, it is treated as YAML content (decoded as UTF-8).
        - Otherwise, it is assumed to be a file-like stream.

    source_description: Used for address as a 'file_name' in case of string or YAML source

    Returns:
      A dictionary resulting from parsing the YAML and processing it with dict_deserialize().

    Raises:
      ValueError if a string is provided that does not correspond to an existing file.
      TypeError for unsupported types.
    """
    file_name = source_description
    if isinstance(source, Path):
        # Try to open the source as a file path.
        file_name = str(source)
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

    raw_dict = yaml.safe_load(content) or {}
    if log is None:
        log = default_logger()
    root_address = SchemaCtx(addr=[], file=file_name, logger=log)
    root = ContextCfg(raw_dict, root_address)
    return dict_deserialize(root)


def convert_value(obj: NodeSchema):
    """
    Recursively convert an object for YAML serialization.

    - If obj is an instance of an attrs class, convert it to a dict using
      attrs.asdict with this function as the value_serializer.
    - If obj is a dict, list, or tuple, process its elements recursively.
    - For basic types (int, float, str, bool, None), return the value as is.
    - Otherwise, return the string representation of obj.
    """
    if isinstance(obj, ContextCfg):
        raise obj.schema_ctx.error(f"Leaking ContextCfg: {obj.value()}")

    if hasattr(obj, "asdict"):
        return obj.asdict(
            value_serializer=lambda inst, field, value: convert_value(value),
            filter=lambda attribute, value: attribute not in {"_address", "_variables"} )
    elif attrs.has(obj):    # for sam reason attrs.has is true also for non attrs classes Variable, Coord
        # Serialize attrs instances, serialize values recursively.
        return attrs.asdict(
            obj,
            value_serializer=lambda inst, field, value: convert_value(value),
            filter=lambda attribute, value: attribute.name != "_address",
        )
    elif isinstance(obj, dict):
        return {k: convert_value(v) for k, v in obj.items()}
    elif hasattr(obj, "dtype"):
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
