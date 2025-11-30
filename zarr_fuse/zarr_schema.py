import re
from functools import cached_property
from typing import *

import pandas
import yaml
import attrs
import numpy as np
from pathlib import Path

from . import __version__
from . import units
from . import logger as zf_logger
from . dtype_converter import to_typed_array, DType, make_na
from .schema_ctx import SchemaCtx, ContextCfg, default_logger, AddressMixin, NoDefault

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




# ----------------------- Converters & core classes ----------------------- #




Scalar = bool | int | float | str
Unit = str | units.DateTimeUnit
RangeTuple = None | List[None] | Tuple[Scalar, Scalar] | Tuple[Scalar,Scalar, Unit]

def unit_instance(cfg: ContextCfg, default_unit: units.UnitType=NoDefault) -> Unit:
    """
    Create instance of pint.Unit for a string intput or
    instance of DateTimeUnit for the dict input.."""
    unit, schema_ctx = cfg.value(), cfg.schema_ctx
    if unit is None:
        if default_unit is NoDefault:
            schema_ctx.error(f"Obligatory unit was not specified.")
        else:
            return default_unit
    elif isinstance(unit, str):
        try:
            u = units.Unit(unit)
            return u
        except Exception as e:
            cfg.schema_ctx.error(f"Invalid unit string: {unit}. Pint error: {e}")
    elif isinstance(unit, dict):
        try:
            return units.DateTimeUnit(**unit)
        except Exception as e:
            cfg.schema_ctx.error(f"Invalid DateTimeUnit dict: {unit}. Error: {e}")
    cfg.schema_ctx.error(f"Unsupported unit: {unit}")


@attrs.define
class DiscreteRange(AddressMixin):
    codes_to_labels: np.ndarray

    @staticmethod
    def from_cfg(cfg: ContextCfg, source_col: str, convert_fn, na_value):
        if isinstance(cfg.value(), str):
            # read values from a CSV file
            # TODO: for large discrete value arrays we should store them as a zarr array not in attrs
            path = Path(cfg.schema_ctx.file) / cfg.value()
            df = pandas.read_csv(path)
            values = convert_fn(df[source_col])
        else:
            values = np.array(cfg.value())
        # Use index zero for na_value
        values = np.concatenate(([[na_value], values]))
        return DiscreteRange(values)

    @property
    def na_value(self):
        return self.codes_to_labels[0]

    def asdict(self, value_serializer, filter):
        return {'discrete': list(self.codes_to_labels[1:])}  # skip NaN

    @cached_property
    def labels_to_codes(self) -> dict[Any, int]:
        return {lab: i for i, lab in enumerate(self.codes_to_labels)}  # NaN is code 0

    def encode(self, values: Iterable[object]) -> np.ndarray:
        # TODO: use Pndas to do encode fast
        # single line: labels_to_codes.get(..., -1) in a comprehension
        return np.asarray([self.labels_to_codes.get(v, 0) for v in values])

    def decode(self, codes: Iterable[int]) -> np.ndarray:
        # codes_to_labels has NaN at index 0; shift codes by +1 and index
        c = np.asarray(codes, dtype=np.int64)
        return self.codes_to_labels[c]

class InfRange:
    def encode(self, values: np.ndarray) -> np.ndarray:
        return values

    def decode(self, values: np.ndarray) -> np.ndarray:
        return values

    def asdict(self, value_serializer, filter):
        return None




@attrs.define
class Interval(AddressMixin):
    """
    Start and end could be None to represent open intervals.
    TODO: unit without type is inconsistent
    """
    start: Any
    end: Any
    unit: Unit

    @classmethod
    def from_list(cls, cfg: ContextCfg, default_unit):
        lst, schema_ctx = cfg.value(), cfg.schema_ctx

        if lst is None:     # empty interval
            return cls(-np.inf, -np.inf, default_unit)

        if not isinstance(lst, list):
            lst = [lst]
        if len(lst) == 0:   # full range
            return cls(-np.inf, np.inf, default_unit)
        if len(lst) == 1:   # single point
            lst = 2 * lst

        if len(lst) > 3:
            raise cfg.schema_ctx.error(f"Invalid interval specification: {lst}")
        if len(lst) == 2:   # start, end
            unit = default_unit
        else: # len(lst) == 3: # start, end, unit
            unit = unit_instance(cfg.get(2, None), default_unit)
        return cls(lst[0], lst[1], unit)


    @classmethod
    def step_limits(cls, cfg, default_unit):
        # backward compatible
        if cfg.cfg is None:
            cfg.cfg = "no_new"
        if isinstance(cfg.cfg, list) and len(cfg.cfg) == 0:
            cfg.cfg = "any_new"

        if isinstance(cfg.cfg, str):
            if cfg.cfg == "no_new" :
                return cls(-np.inf, -np.inf, default_unit)
            elif cfg.cfg == "any_new":
                return cls(-np.inf, np.inf, default_unit)
            else:
                cfg.schema_ctx.error(f"Invalid step_limits specification: {cfg.cfg}")
        if isinstance(cfg.cfg, list):
            start = cfg.cfg[0]
            end = cfg.cfg[1]
            unit = unit_instance(cfg.get(2, None), default_unit)
        else:
            assert isinstance(cfg.cfg, dict)
            start = cfg.cfg['start']
            end = cfg.cfg['end']
            unit = unit_instance(cfg.get("unit", None), default_unit)
        if start > end:
            cfg.schema_ctx.error(f"Invalid step_limits specification: start {start} > end {end}")
        return cls(start, end, unit)

    def no_new(self):
        return self.start == -np.inf and self.end == -np.inf

    def any_new(self):
        return self.start == -np.inf and self.end == np.inf

    def asdict(self, value_serializer, filter):
        # Here we serialize set_limits, reproducing special strings.
        if self.any_new():
            return "any_new"
        elif self.no_new():
            return "no_new"
        else:
            return [self.start, self.end, value_serializer(self, 2, self.unit)]



@attrs.define
class IntervalRange(Interval):
    """
    TODO: support and test DateTime ranges.
    """
    def asdict(self, value_serializer, filter):
        return {'interval': Interval.asdict(self, value_serializer, filter)}

    def encode(self, values: np.ndarray) -> np.ndarray:
        # single line: labels_to_codes.get(..., -1) in a comprehension

        correct_mask = (self.start <= values)  &  (values <= self.end)
        if np.all(correct_mask):
            return values
        raise ValueError(f"Values out of range [{self.start}, {self.end}]: {values[~correct_mask]}")

    def decode(self, codes: np.ndarray) -> np.ndarray:
        # codes_to_labels has NaN at index 0; shift codes by +1 and index
        c = np.asarray(codes, dtype=np.int64)
        return codes


Quantity = units.Quantity | units.DateTimeQuantity


@attrs.define
class Variable(AddressMixin):
    """
    Definition of a Variable in the zarr-fuse schema.
    
    TODO: 
    - allow custom attrs for variables and coordinates
    """

    def __init__(self, dict:ContextCfg):
        self._address : SchemaCtx = dict.schema_ctx
        assert self._address is not None

        # Obligatory attributes
        self.name: str = dict["name"].value()
        self.coords: Union[str, List[str]] = dict.get("coords").value()
        if isinstance(self.coords, str):
            self.coords = [self.coords]
        unit_item = dict.get("unit", None)
        self.unit: Optional[Unit] = unit_instance(unit_item, units.NoneUnit())

        # Optional attributes
        self.type: DType = DType.from_cfg(dict.get("type", None))
        na_cfg = dict.get("na_value", self.type.default_na).cfg
        self.na_value : Optional[Any] = make_na(na_cfg, self.type.dtype)
        self.description: Optional[str] = dict.get("description", None).value()
        self.df_col: Optional[str] = dict.get("df_col", self.name).value()

        source_unit_item = dict.get("source_unit", None)
        self.source_unit: Optional[Unit] = unit_instance(source_unit_item, self.unit)

        # Depends on unit, df_col, source_unit
        self.range: DiscreteRange | IntervalRange \
            = self.range_instance(dict.get("range", None))

    @property
    def dtype(self):
        return self.type.dtype

    def valid_mask(self, array: np.ndarray) -> np.ndarray:
        if self.na_value is None:
            return np.full(array.shape, True, dtype=bool)
        elif (self.na_value != self.na_value):
            # alternativly use numexpr
            na_array = np.full_like(array, self.na_value, dtype=self.dtype)
            return (array != na_array) & (array == array)
        else:
            return array != self.na_value

    def _zarr_keys(self):
        return ['unit', 'type', 'range', 'description', 'df_col', 'source_unit']

    def zarr_attrs(self):
        return { k:convert_value(getattr(self, k)) for k in self._zarr_keys()}

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
            return InfRange()

        if 'discrete' in range_dict:
            convert_fn = lambda vals: self.make_quantity(vals).magnitude
            return DiscreteRange.from_cfg(dict_ctx['discrete'], self.df_col, convert_fn, self.na_value)
        elif 'interval' in range_dict:
            return IntervalRange.from_list(dict_ctx['interval'], self.unit)

        schema_ctx.error(f"Invalid range specification: {range_dict}")

    @staticmethod
    def _opt_arg(arg, default):
        return default if arg is None else arg

    def convert_values(self, values, from_unit=None, dtype = None, to_unit=None, range = None):
        """
        Convert from source units to the variable's unit, check type
        :param values:
        :return:
        """
        quantity = self._make_quantity(values, from_unit=from_unit, dtype=dtype)
        to_unit = self._opt_arg(to_unit, self.unit)
        range = self._opt_arg(range, self.range)

        q_new = quantity.to(to_unit)
        q_new = q_new.magnitude
        q_new = range.encode(q_new)
        return q_new

    def _make_quantity(self, values: np.ndarray, from_unit=None, dtype = None):
        """
        Convert raw values to a pint.Quantity or DateTimeQuantity in the variable's unit.
        """
        from_unit = self._opt_arg(from_unit, self.source_unit)
        dtype = self._opt_arg(dtype, self.dtype)

        if isinstance(from_unit, units.DateTimeUnit):
            # DateTime specialization
            quantity = units._create_dt_quantity(values, from_unit, log=self._address)
        else:
            if dtype is not None:
                values = to_typed_array(values, dtype, self._address)

            # Pint specialization when a unit string is provided
            assert isinstance(from_unit, (units.Unit, units.NoneUnit))
            quantity = units.Quantity(values, from_unit)
        return quantity

    def magnitude(self, q: np.ndarray | Quantity) -> np.ndarray:
        """
        Return the magnitude of the provided quantity, converting to the variable's unit if needed.
        """
        if isinstance(q, (units.Quantity, units.DateTimeQuantity)):
            return q.magnitude
        else:
            return q

    def quantity(self, q: np.ndarray | Quantity) \
            -> Quantity:
        """
        Return the provided quantity, converting to the variable's unit if needed.
        """
        if isinstance(q, (units.Quantity, units.DateTimeQuantity)):
            return q.to(self.unit)
        else:
            return self._make_quantity(q, from_unit=self.unit)

    def encode(self, values: np.ndarray | Quantity) -> np.ndarray:
        return self.range.encode(self.magnitude(values))

    def decode(self, values: np.ndarray) -> Quantity:
        return self.quantity(self.range.decode(values))

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

        step_item = cfg.get("step_limits", "any_new")  # None value allowed
        default_step_unit = self.unit.delta_unit() # For DateTimeUnit the incremental/step unit is a pint.Unit of time.
        self.step_limits = Interval.step_limits(step_item, default_step_unit)

        self.sorted : bool = cfg.get('sorted', not self.is_composed()).value()

        # May be explicit in the future. Namely, if we support interpolation of sparse coordinates.
        self._variables = None # set in DatasetSchema.__init__

    def _zarr_keys(self):
        return super()._zarr_keys() + ['composed', 'chunk_size', 'step_limits', 'sorted']

    def is_composed(self):
        return len(self.composed) > 1

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

    def __init__(self, attrs: ContextCfg, vars: ContextCfg, coords: ContextCfg):
        self._address = attrs.schema_ctx.parent()
        self.ATTRS = attrs.value()
        self.ATTRS['VERSION'] = __version__

        # Backward compatible definition of coords as VARS.
        for coord in coords.keys():
            if coord in vars:
                self.warn(
                    f"OBSOLETE. Coordinate '{coord}' is defined both in VARS and COORDS. ",
                    subkeys=["COORDS", coord],
                )
                coords[coord].value().update(vars[coord].cfg)
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
        return (
                self.ATTRS == {'VERSION':__version__}
                and not self.COORDS
                and not self.VARS)


    def zarr_attrs(self):
        attrs = { k:convert_value(v) for k,v in self.ATTRS.items()}
        return attrs

@attrs.define
class NodeSchema(AddressMixin):
    _address: SchemaCtx = attrs.field(alias="_address", repr=False, eq=False)
    ds: DatasetSchema
    groups: Dict[str, "NodeSchema"] = attrs.field(factory=dict)

    @classmethod
    def make_empty(cls):
        return build_nodeschema(ContextCfg({}, SchemaCtx([])))

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

def build_nodeschema(content: ContextCfg) -> NodeSchema:
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
        key: build_nodeschema(content[key])
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
    version = raw_dict.get('ATTRS', {}).get('VERSION', '0.2.0')
    root_address = SchemaCtx(addr=[], version=version, file=file_name, logger=log)
    root = ContextCfg(raw_dict, root_address)
    return build_nodeschema(root)


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
    root_node_dict = convert_value(node_schema)
    root_node_dict['ATTRS']['VERSION'] = __version__
    content = yaml.safe_dump(root_node_dict, sort_keys=False)
    if path is None:
        return content
    else:
        with Path(path).open("w", encoding="utf-8") as file:
            file.write(content)
    return content
