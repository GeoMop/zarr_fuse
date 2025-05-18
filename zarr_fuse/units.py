import re
import numpy as np
import pint
import datetime
from dateutil import parser, tz
import attrs
import time

ureg = pint.UnitRegistry()

def _tz_converter(val):
    """
    Convert tz specification to a datetime.tzinfo object.
    Accepts None, tzinfo, '+HH:MM' or '-HH:MM', or named zones.
    """
    if val is None or isinstance(val, datetime.tzinfo):
        return val
    if isinstance(val, str):
        m = re.match(r'([+-])(\d{2}):(\d{2})$', val)
        if m:
            sign = 1 if m.group(1) == '+' else -1
            hours, mins = int(m.group(2)), int(m.group(3))
            offset = datetime.timedelta(hours=hours, minutes=mins) * sign
            return datetime.timezone(offset)
        tzinfo = tz.gettz(val)
        if tzinfo is None:
            raise ValueError(f"Unknown timezone spec '{val}'")
        return tzinfo
    raise TypeError(f"Invalid tz type: {type(val)}")

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
    """
    tick: str = 'us'
    tz: datetime.tzinfo = attrs.field(default=None, converter=_tz_converter)
    dayfirst: bool = False
    yearfirst: bool = False

    @property
    def tz_shift(self) -> float:
        """Hours offset from UTC."""
        if self.tz is None:
            return 0.0
        offset = self.tz.utcoffset(datetime.datetime.now())
        return offset.total_seconds() / 3600.0

class DateTimeQuantity:
    """
    Wraps a numpy.datetime64 array with its DateTimeUnit config.
    """
    def __init__(self, values: np.ndarray, dt_unit: DateTimeUnit):
        if not np.issubdtype(values.dtype, np.datetime64):
            raise TypeError(f"values.dtype must be datetime64, got {values.dtype}")
        expected = np.dtype(f'datetime64[{dt_unit.tick}]')
        if values.dtype != expected:
            raise ValueError(f"values.dtype {values.dtype} does not match unit tick {dt_unit.tick}")
        self._values = values
        self._unit = dt_unit

    @property
    def magnitude(self) -> np.ndarray:
        """Underlying numpy.datetime64 array."""
        return self._values

    @property
    def units(self) -> str:
        return f"datetime64[{self._unit.tick}]"

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
                f"unit={self.units} tz_shift={self._unit.tz_shift}h>")


def create_quantity(values, from_unit):
    """
    Create pint.Quantity for numeric or DateTimeQuantity for datetime strings.
    """
    arr = np.asarray(values)
    try:
        if arr.dtype.kind in ('U', 'S', 'O'):
            arr = arr.astype(float)
        return ureg.Quantity(arr, from_unit)
    except Exception:
        pass
    if not isinstance(from_unit, dict):
        raise TypeError("from_unit must be dict for datetime parsing")
    dt_unit = DateTimeUnit(**from_unit)
    parsed = []
    for val in values:
        dt = parser.parse(str(val),
                          dayfirst=dt_unit.dayfirst,
                          yearfirst=dt_unit.yearfirst,
                          tzinfos=TZINFOS)
        # If no explicit tz, assign dt_unit tz (or UTC if none)
        if dt.tzinfo is None:
            target_tz = dt_unit.tz or datetime.timezone.utc
            dt = dt.replace(tzinfo=target_tz)
        # Convert any explicit tz to dt_unit.tz (or UTC)
        final_tz = dt_unit.tz or datetime.timezone.utc
        dt_utc = dt.astimezone(final_tz)
        # Drop tzinfo to store as numpy.datetime64 local times
        dt_naive = dt_utc.replace(tzinfo=None)
        parsed.append(dt_naive)
    np_dates = np.array([np.datetime64(dt, dt_unit.tick) for dt in parsed])
    return DateTimeQuantity(np_dates, dt_unit)


