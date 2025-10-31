import logging
import re
from typing import *

import attrs
import numpy as np
from zarr_fuse.schema_ctx import ContextCfg, SchemaCtx

log = logging.getLogger(__name__)

# ---- Spec parsing ----



def type_code(t):
    """Return a short type code for dtype t."""
    t = np.dtype(t)
    codes = [('1B', np.bool_), ('2I', np.integer), ('3F', np.floating), ('4C', np.complexfloating), ('5U', np.str_)]
    for c, t_super in codes:
        if np.issubdtype(t, t_super):
            return (c, t.itemsize)
    return ('6O', 0)

# ---- Trimming detector ----
def may_trim(src: np.dtype, dst: np.dtype) -> bool:
    """Return True if casting src->dst could change values; intentionally simple/fast."""
    s, t = np.dtype(src), np.dtype(dst)
    return type_code(t) < type_code(s)


@attrs.define
class TrimmedArrayWarning(Warning):
    trimmed_values: np.ndarray

    @property
    def preview(self) -> str:
        return  ", ".join(repr(v) for v in self.trimmed_values[:10])

    def __str__(self):
        size = len(self.trimmed_values)
        if size > 10:
            return f"Trimmed values detected: [{self.preview}, ... (size= {size} more)]"
        return f"Trimmed values detected: [{self.preview}]"

def _is_str(a) -> bool:
    return a.dtype.kind in ("S", "U", "O")

def _is_numeric(a) -> bool:
    return np.issubdtype(a.dtype, np.number)

def _trim_change_mask(arr: np.ndarray, out: np.ndarray) -> np.ndarray:
    """Return boolean mask of elements that changed from arr to out."""
    if _is_numeric(arr) and _is_numeric(out):
        out_back = out.astype(arr.dtype)
        eq = (arr == out_back) | (np.isnan(arr) & np.isnan(out_back))
        return ~eq


    if _is_str(arr) and _is_str(out):
        assert out.dtype.kind == "U"
        out_str_len = out.dtype.itemsize // 4   # Unicode used exclusively.
        return np.char.str_len(arr.astype("U", copy=False)) > out_str_len

    return np.zeros(arr.shape, dtype=bool)   # no trims


 # # Enforce unit constraint: when unit is provided, bool and str[n] are not allowed.
  #   if unit is not None and target_dtype is not None:
  #       if target_dtype == np.dtype(bool):
  #           raise ValueError("Boolean dtype is not allowed when 'unit' is provided.")
  #       if str_len is not None:
  #           raise ValueError("Fixed-length string dtype 'str[n]' is not allowed when 'unit' is provided.")
_STR_SPEC = re.compile(r"^str(?:\[(\d+)\])?$")
type_mapping = {
    "bool": np.int8,
    "uint": np.uint64,
    "int": np.int64,
    "int8": np.int8,
    "int32": np.int32,
    "int64": np.int64,
    "uint8": np.uint8,
    "uint32": np.uint32,
    "uint64": np.uint64,
    "float": np.float64,
    "float64": np.float64,
    "complex": np.complex64,
}
_PREFERRED_NAME = {np.dtype(v): k for k,v in type_mapping.items()}


def make_na(val, dt):
    """
    Parse NA sentinel value for given dtype.
    :param val:
    :param dt:
    :return:
    """
    if dt is None:
        return val

    dtype = np.dtype(dt)
    if isinstance(val, str):
        if dtype.kind in  {"i", "u"}:
            if val == "max_int":
                return np.iinfo(dtype).max
            if val == "min_int":
                return np.iinfo(dtype).min
        if dtype.kind in  {"U"}:
            return np.asarray([val], dtype=dtype)[0]
        raise ValueError(f"Unknown NA sentinel string: {val}")
    else:
        return np.asarray([val], dtype=dtype)[0]


_list_of_defaults = [
    [np.nan, "float32", "float64"],
    [np.nan + 1j * np.nan, "complex64", "complex128"],
    ["max_int", "uint8", "uint16", "uint32", "uint64"],
    ["min_int", "int8", "int16", "int32", "int64"],
    [np.datetime64("NaT"), "datetime64[ns]"],
    [np.timedelta64("NaT"), "timedelta64[ns]"]
]
DEFAULT_NA_BY_DTYPE = {
    np.dtype(dt): make_na(val_list[0], dt)
    for val_list in _list_of_defaults
    for dt in val_list[1:]
}

DEFAULT_STR_LEN = 32

@attrs.define
class DType:
    dtype: Optional[np.dtype]

    @classmethod
    def from_cfg(cls, cfg: ContextCfg) -> 'DType':
        _type, _ctx = cfg.value(), cfg.schema_ctx
        if _type is None:
            return cls(None)

        _type = _type.strip()
        m = _STR_SPEC.match(_type)
        if m:
            n = m.group(1)
            if n is None:
                _ctx.warning("Used type 'str' without length specification, assuming 'str[32]. Zarr-fuse only supports fixed string values in arrays.'", stacklevel=3)
                n = DEFAULT_STR_LEN
            n = int(n)
            if n <= 0:
                _ctx.warning(f"Invalid type 'str[{n}]',  n > 0 required. Using default length n={DEFAULT_STR_LEN}.", stacklevel=3)
                n = DEFAULT_STR_LEN
            return cls(np.dtype(f"<U{n}"))
        _dtype = type_mapping.get(_type,  None)
        if _dtype is None:
            _ctx.error(f"Unsaported value type: {_type}")
        return cls(np.dtype(_dtype))

    def asdict(self, value_serializer, filter):
        dt = self.dtype
        if dt is None:
            return None

        # Special-case numpy Unicode dtype -> 'str[n]'
        if dt.kind == "U":
            # n chars = itemsize / itemsize_of('<U1')
            n = dt.itemsize // np.dtype("<U1").itemsize
            return f"str[{n}]"

        # Simple dict lookup for known scalar dtypes
        return _PREFERRED_NAME.get(dt, str(dt))

    @property
    def default_na(self):
        """
        Return the default NA sentinel for a NumPy dtype under the policy above.
        - signed ints: ~0  (all bits set -> -1)
        - unsigned ints: max_int
        - unicode strings: U+FFFF repeated to the fixed width
        Falls back to dict for floats/complex/bool/NaT families.
        """
        if self.dtype is None:
            return None
        dt = np.dtype(self.dtype)

        # direct table lookups (floats, complex, bool, dt64/timedelta64)
        if dt in DEFAULT_NA_BY_DTYPE:
            return DEFAULT_NA_BY_DTYPE[dt]
        if dt.kind == "U":
            n_chars = dt.itemsize // np.dtype("<U1").itemsize  # = itemsize // 4
            return "\uFFFF" * n_chars  # length == n_chars


# ---- Main API ----
def to_typed_array(x: Any, target_dtype: Optional[np.dtype], ctx:'SchemaCtx') -> np.ndarray:
    """
    1) Convert without checks.
    2) If cast could trim, compare original vs converted using np.array_equal(equal_nan=False).
    3) If different, log once with ORIGINAL values that changed, and return the converted array.
    """
    if target_dtype is None:
        return np.asarray(x)
    # 'ZK1-10DL',
    arr = np.asarray(x)
    out = np.asarray(arr, dtype=target_dtype)

    # could out.dtype trim?
    if not may_trim(arr.dtype, out.dtype):
        return out

    trim_mask = _trim_change_mask(arr, out)
    if np.any(trim_mask):
        trimmed_values = arr[trim_mask]
        ctx.warning(TrimmedArrayWarning(trimmed_values))

    return out
