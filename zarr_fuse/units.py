import re
from functools import cached_property
from typing import Iterable, Any

import numpy as np
import pint
from pint import UndefinedUnitError
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

class Unit(pint.Unit):
    def asdict(self, value_serializer, filter):
        return str(self)

    def delta_unit(self):
        return self

    def delta_dtype(self, dtype):
        return dtype

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

    def delta_unit(self):
        return Unit(self.tick)  # TODO: change from 'str' to pint.Unit in DataTimeUnit

    def delta_dtype(self, dtype):
        return np.dtype(f'timedelta64[{self.tick}]')

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





