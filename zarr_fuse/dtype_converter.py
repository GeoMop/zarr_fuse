import logging
import re
from typing import *

import attrs
import numpy as np

log = logging.getLogger(__name__)

# ---- Spec parsing ----
_STR_SPEC = re.compile(r"^str\[(\d+)\]$")
type_mapping = {
    "bool": np.bool,
    "int": np.int64,
    "int8": np.int8,
    "int32": np.int32,
    "int64": np.int64,
    "float": np.float64,
    "float64": np.float64,
    "complex": np.complex64,
}
def _parse_dtype_spec(spec: Optional[str], log: logging.Logger) -> Tuple[Optional[np.dtype], Optional[int]]:
    if spec is None or (isinstance(spec, str) and spec.strip().lower() == "none"):
        return None, None

    spec = spec.strip()
    m = _STR_SPEC.match(spec)
    if m:
        n = int(m.group(1))
        if n <= 0:
            log.error("str[n] requires n > 0.")
            n = 8
        return np.dtype(f"<U{n}"), n
    if spec not in type_mapping:
        log.error(f"Unsaported value type: {spec}")
    return np.dtype(type_mapping[spec]), None

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


# ---- Main API ----
def to_typed_array(x: Any, dtype: str, log: logging.Logger) -> np.ndarray:
    """
    1) Convert without checks.
    2) If cast could trim, compare original vs converted using np.array_equal(equal_nan=False).
    3) If different, log once with ORIGINAL values that changed, and return the converted array.
    """
    target_dtype, _ = _parse_dtype_spec(dtype, log)
    arr = np.asarray(x)

    # 1) conversion first (NumPy will raise on impossible casts)
    out = arr if target_dtype is None else arr.astype(target_dtype, copy=False)

    # 2) quick heuristic gate
    if target_dtype is None or not may_trim(arr.dtype, out.dtype):
        return out

    # 3) whole-array equality (NaNs count as changes: equal_nan=False)
    if np.array_equal(arr, out, equal_nan=False):
        return out

    # Build a simple elementwise mask to collect originals that changed.
    # No special NaN handling: NaN != NaN will be considered a change (by design).
    arr_obj = np.asarray(arr, dtype=object)
    out_obj = np.asarray(out, dtype=object)
    changed_mask = arr_obj != out_obj

    trimmed_values = arr_obj[changed_mask]
    if trimmed_values.size > 0:
        log.warning(TrimmedArrayWarning(trimmed_values))

    return out
