import logging
import re
from typing import *

import attrs
import numpy as np
from zarr.core.dtype import dtype

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
