import re
from functools import cached_property
from typing import Iterable, Any

import numpy as np
import pint
import datetime
import dateutil
import attrs
import time


ureg = pint.UnitRegistry()

ureg.define('bool = []')
ureg.define('boolean = bool')          # alias
ureg.define('true  = 1 bool')         # allow parsing "true"
ureg.define('false = 0 bool')


# Map common timezone abbreviations to fixed-offset tzinfo
# Daylight saving time intantionaly forbiden to avoid duplicit times during transition.
# Build TZINFOS mapping dynamically for all available fixed-offset (non-DST) IANA timezones
import zoneinfo

def build_tzinfos():
    """
     Return dict mapping common timezone abbreviations to fixed-offset seconds (no DST), using pytz.
     """
    tzinfos = {}
    winter_instance = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
    summer_instance = datetime.datetime(2000, 7, 1, tzinfo=datetime.timezone.utc)
    inconsistent = set()
    for zone in zoneinfo.available_timezones():
        tzobj = zoneinfo.ZoneInfo(zone)
        # utcoffset and dst at current UTC time
        # offset = tzobj.utcoffset(now)
        # dst = tzobj.dst(now)
        # skip zones with DST
        # if dst != datetime.timedelta(0) or offset is None:
        #    continue
        # get abbreviation like 'CET'
        for instance in [winter_instance, summer_instance]:
            abbr = instance.astimezone(tzobj).tzname()
            if not abbr or len(abbr) > 5:
                continue
            # map abbreviation to offset seconds
            if abbr in tzinfos:
                existing = tzinfos[abbr].utcoffset(instance)
                new = tzobj.utcoffset(instance)
                if existing != new:
                    inconsistent.add(abbr)
            tzinfos[abbr] = tzobj
    print("Removing inconsistent TZ codes:", inconsistent)
    for code in inconsistent:
        tzinfos.pop(code)
    return tzinfos

TZINFOS = build_tzinfos()


@attrs.define
class DateTimeUnit:
    """
    Configuration for datetime parsing and storage.
    Stores only a plain‐string `_tz` so that YAML/attrs can serialize without special representers.
    """
    tick: str = 'us'
    tz: str | None = None
    dayfirst: bool = False
    yearfirst: bool = True

    @property
    def tzinfo(self) -> datetime.tzinfo | None:
        """
        Lazily convert the stored zone‐name/_offset string into a tzinfo instance.
        Accepts None, '+HH:MM' or '-HH:MM', or named zones.
        """
        val = self.tz
        if val is None:
            return None

        # offset form ±HH:MM
        m = re.match(r'([+-])(\d{2}):(\d{2})$', val)
        if m:
            sign = 1 if m.group(1) == '+' else -1
            hours, mins = int(m.group(2)), int(m.group(3))
            offset = datetime.timedelta(hours=hours, minutes=mins) * sign
            return datetime.timezone(offset)

        # named zone
        tzinfo = dateutil.tz.gettz(val)
        if tzinfo is None:
            raise ValueError(f"Unknown timezone spec '{val}'")
        return tzinfo

    @property
    def tz_shift(self) -> float:
        """Hours offset from UTC for the current datetime."""
        tzinfo = self.tzinfo
        if tzinfo is None:
            return 0.0
        offset = tzinfo.utcoffset(datetime.datetime.now())
        return offset.total_seconds() / 3600.0

class DateTimeQuantity:
    """
    Wraps a numpy.datetime64 array with its DateTimeUnit config.
    """
    def __init__(self, values: np.ndarray, dt_unit: DateTimeUnit):
        # 1) Ensure it's some kind of datetime64
        if not np.issubdtype(values.dtype, np.datetime64):
            raise TypeError(f"values.dtype must be datetime64, got {values.dtype!r}")

        # 2) Compute the target dtype from dt_unit.tick
        expected = np.dtype(f'datetime64[{dt_unit.tick}]')

        self._values = values.astype(expected)
        self._unit = dt_unit

    @property
    def magnitude(self) -> np.ndarray:
        """Underlying numpy.datetime64 array."""
        return self._values

    # @property
    # def units(self) -> str:
    #     return f"datetime64[{self._unit.tick}]"

    def to(self, target_unit: DateTimeUnit):
        """
        Convert array to another DateTimeUnit, applying timezone shifts.

        Steps:
        1. Subtract source tz shift from original values (in original tick).
        2. Convert to new tick.
        3. Add target tz shift.
        """
        # 1) Subtract source tz shift (minutes)
        shift_from = np.timedelta64(int(self._unit.tz_shift * 60), 'm')
        utc_vals = self._values - shift_from

        # 2) Cast to target tick
        new_arr = utc_vals.astype(f'datetime64[{target_unit.tick}]')

        # 3) Add target tz shift (minutes)
        shift_to = np.timedelta64(int(target_unit.tz_shift * 60), 'm')
        local_vals = new_arr + shift_to

        return DateTimeQuantity(local_vals, target_unit)

    def __add__(self, other):
        """
        Add a timedelta expressed as a pint.Quantity to this DateTimeQuantity.
        """
        if not isinstance(other, pint.Quantity):
            return NotImplemented
        # Ensure dimension is time
        other_sec = other.to('second').magnitude
        # Use time module to handle leap seconds correctly
        # Convert seconds to microseconds
        us = int(time.time() * 0 + other_sec * 1e6)
        delta = np.timedelta64(us, 'us')
        # Shift and return new DateTimeQuantity
        arr_us = self._values.astype('datetime64[us]') + delta
        new_arr = arr_us.astype(f'datetime64[{self._unit.tick}]')
        return DateTimeQuantity(new_arr, self._unit)

    def __repr__(self):
        return (f"<DateTimeQuantity array={self._values!r} "
                f"unit={self._unit}h>")


def _create_dt_quantity(values, dt_unit):
    """
    Create a DateTimeQuantity from a numpy array of datetime64 values and a DateTimeUnit.
    """
    parsed = []
    for val in values:
        dt = dateutil.parser.parse(str(val),
                          dayfirst=dt_unit.dayfirst,
                          yearfirst=dt_unit.yearfirst,
                          tzinfos=TZINFOS)
        # If no explicit tz, assign dt_unit tz (or UTC if none)
        if dt.tzinfo is None:
            target_tz = dt_unit.tzinfo or datetime.timezone.utc
            dt = dt.replace(tzinfo=target_tz)
        # Convert any explicit tz to dt_unit.tz (or UTC)
        final_tz = dt_unit.tzinfo or datetime.timezone.utc
        dt_utc = dt.astimezone(final_tz)
        # Drop tzinfo to store as numpy.datetime64 local times
        dt_naive = dt_utc.replace(tzinfo=None)
        parsed.append(dt_naive)
    np_dates = np.array([np.datetime64(dt, dt_unit.tick) for dt in parsed])
    return DateTimeQuantity(np_dates, dt_unit)


@attrs.define
class CategoricalUnit:
    categories: list[str] = attrs.field(converter=np.unique)
    nan_label: str = "NaN"

    @cached_property
    def labels_to_codes(self) -> dict[Any, int]:
        return {lab: i for i, lab in enumerate([self.nan_label, *self.categories])} # NaN is code 0

    @cached_property
    def codes_to_labels(self) -> np.ndarray:
        # index 0 is the NaN label; real categories follow at +1 offset
        return np.asarray([self.nan_label, *self.categories])

    def arr_to_codes(self, values: Iterable[object]) -> np.ndarray:
        # single line: labels_to_codes.get(..., -1) in a comprehension
        return np.asarray([self.labels_to_codes.get(v, 0) for v in values])

    def arr_from_codes(self, codes: Iterable[int]) -> np.ndarray:
        # codes_to_labels has NaN at index 0; shift codes by +1 and index
        c = np.asarray(codes, dtype=np.int64)
        return self.codes_to_labels[c]

    # def __repr__(self) -> str:
    #     return f"<CategoricalUnit n={len(self.categories)} nan_label={self.nan_label!r}>"


# ---------- Quantity ----------

class CategoricalQuantity:
    """
    Stores categorical data as integer codes (np.int64); -1 denotes NaN.
    """
    def __init__(self, values: Iterable[object], cat_unit: CategoricalUnit):
        self._unit = cat_unit
        arr = np.asarray(values)
        if arr.dtype.kind in ('i', 'u'):
            self._codes = arr.astype(np.int64, copy=False)
        else:
            self._codes = cat_unit.arr_to_codes(values).astype(np.int64, copy=False)

    @property
    def magnitude(self) -> np.ndarray:
        return self._codes

    # @property
    # def units(self) -> tuple[str, ...]:
    #     return self._unit.index_to_label

    def to(self, target: CategoricalUnit | str):
        if target is str:
            return self._unit.arr_from_codes(self._codes)
           # remap via labels -> target codes (unknowns map to -1)
        labels = self._from_codes(self._codes, self._unit)
        new_codes = self._to_codes(labels, target)
        return CategoricalQuantity(new_codes, target)

    def __len__(self) -> int:
        return self._codes.shape[0]

    def __repr__(self) -> str:
        return f"<CategoricalQuantity n={len(self)} unit={self._unit!r}>"
# def create_quantity(values, from_unit, to_unit):
#     """
#     Create pint.Quantity for numeric or DateTimeQuantity for datetime strings.
#     """
#     arr = np.asarray(values)
#     if isinstance(from_unit, DateTimeUnit):
#         return _create_dt_quantity(arr, from_unit).to(to_unit)
#
#     if arr.dtype.kind in ('f', 'U', 'S', 'O'):
#         arr = arr.astype(float)
#     if arr.dtype.kind in ('O')
#
#         q_new = source_quantity_arr.to(var.unit)
#
#     return ureg.Quantity(arr, from_unit).to(to_unit)

_TRUE = {"true", "1", "t", "yes", "y"}
_FALSE = {"false", "0", "f", "no", "n", ""}

# ---- base converters (dict-dispatched) --------------------------------

def _to_float(values):
    return np.asarray(values, dtype=float)

def _to_int(values):
    # tolerant of "3.0" strings: float→int
    return _to_float(values).astype(int)

def _to_bool(values):
    a = np.asarray(values, dtype=object)
    out = []
    for v in a:
        if isinstance(v, (int, float, np.number)):
            out.append(bool(v))
        elif isinstance(v, str):
            s = v.strip().lower()
            if s in _TRUE: out.append(True)
            elif s in _FALSE: out.append(False)
            else: raise ValueError(f"Cannot parse bool from '{v}'")
        else:
            out.append(bool(v))
    return np.asarray(out, dtype=bool)

def _to_str(values):
    return np.asarray(values, dtype=object).astype(str)

_TO_CONVERTER = {
    "float": _to_float,
    "int":   _to_int,
    "bool":  _to_bool,
    "str":   _to_str,
}

def _normalize_type_name(name):
    if not isinstance(name, str):
        return None
    key = name.strip().lower()
    return key if key in _TO_CONVERTER else None


# ---- public API: keep original signature ------------------------------

def create_quantity(values, from_unit, to_unit):
    """
    Create pint.Quantity for numeric -> unit conversion,
    DateTimeQuantity for DateTimeUnit conversions,
    or do base type conversion when `to_unit` is a type name:
      'bool' | 'int' | 'float' | 'str'
    """
    # DateTime path
    if isinstance(to_unit, DateTimeUnit):
        # assume to_unit is a DateTimeUnit-compatible target
        arr = np.asarray(values)
        return _create_dt_quantity(arr, from_unit).to(to_unit)

    if isinstance(to_unit, CategoricalUnit):
        arr = np.asarray(values)
        return CategoricalQuantity(arr, from_unit).to(to_unit)

    # Primary resolution by to_unit-as-type
    target_type = _normalize_type_name(to_unit)
    if target_type is not None:
        return _TO_CONVERTER[target_type](values)

    # Pint path (float arrays)
    # Only when both from_unit and to_unit are provided as pint units/strings.
    if from_unit is not None and to_unit is not None:
        payload = _to_float(values)
        return ureg.Quantity(payload, from_unit).to(to_unit)

    # 4) Fallback: just float array (keeps behavior sane if only from_unit given)
    return _to_float(values)

def step_unit(unit: str):
    """
    Return the step unit for a given unit.
    """
    q = create_quantity([], unit)
    if isinstance(q, DateTimeUnit):
        return q.tick
    else:
        assert isinstance(q, pint.Quantity), "unit must be a string or DateTimeUnit"
        return unit