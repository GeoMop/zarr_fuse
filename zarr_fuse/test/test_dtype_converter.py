# test_typed_array.py
import numpy as np
import pytest

import zarr_fuse.dtype_converter as ta


# --- Simple in-memory logger that captures objects passed to .error/.warning ----
class DummyLogger:
    def __init__(self, name="dummy"):
        self.name = name
        self.records = []   # list of tuples: (level, obj)

    def error(self, obj, *args, **kwargs):
        self.records.append(("ERROR", obj))

    def warning(self, obj, *args, **kwargs):
        self.records.append(("WARNING", obj))

    # helpers
    def count_level(self, level: str) -> int:
        return sum(1 for lvl, _ in self.records if lvl == level)

    def has_warning_instance(self, cls) -> bool:
        return any(lvl == "WARNING" and isinstance(obj, cls) for lvl, obj in self.records)

    def clear(self):
        self.records.clear()


# --- _parse_dtype_spec --------------------------------------------------------
def test__parse_dtype_spec():
    log = DummyLogger()

    # None / 'None' -> infer
    dt, n = ta._parse_dtype_spec(None, log)
    assert dt is None and n is None
    dt, n = ta._parse_dtype_spec("None", log)
    assert dt is None and n is None
    assert log.count_level("ERROR") == 0  # no noise for valid cases

    # Valid basic specs
    basics = [
        ("bool", np.bool),
        ("int", np.int64),
        ("int8", np.int8),
        ("int32", np.int32),
        ("int64", np.int64),
        ("float", np.float64),
        ("float64", np.float64),
        ("complex", np.complex64),
    ]
    for spec, expected in basics:
        dt, n = ta._parse_dtype_spec(spec, log)
        assert dt == np.dtype(expected) and n is None
    assert log.count_level("ERROR") == 0

    # Valid str[n]
    dt, n = ta._parse_dtype_spec("str[5]", log)
    assert dt == np.dtype("<U5") and n == 5
    assert log.count_level("ERROR") == 0

    # Nonpositive str length -> ERROR logged, coerces to U8
    log.clear()
    dt, n = ta._parse_dtype_spec("str[0]", log)
    assert dt == np.dtype("<U8") and n == 8
    assert log.count_level("ERROR") >= 1   # at least one error logged

    # Unsupported type -> ERROR logged, then KeyError from mapping
    log.clear()
    with pytest.raises(KeyError):
        ta._parse_dtype_spec("nope", log)
    assert log.count_level("ERROR") >= 1


# --- type_code ----------------------------------------------------------------
def test_type_code():
    # Category ordering: bool < int < float < complex < unicode < object
    c_bool        = ta.type_code(np.bool_)
    c_int32       = ta.type_code(np.int32)
    c_float64     = ta.type_code(np.float64)
    c_complex128  = ta.type_code(np.complex128)
    c_u10         = ta.type_code(np.dtype("<U10"))
    c_obj         = ta.type_code(object)
    assert c_bool < c_int32 < c_float64 < c_complex128 < c_u10 < c_obj

    # Within-class by itemsize (e.g., U5 < U10)
    assert ta.type_code(np.dtype("<U5")) < ta.type_code(np.dtype("<U10"))


# --- may_trim -----------------------------------------------------------------
def test_may_trim():
    assert ta.may_trim(np.float64,  np.int32)     is True   # float -> int
    assert ta.may_trim(np.complex128, np.float64) is True   # complex -> float
    assert ta.may_trim(np.int64,    np.int32)     is True   # narrowing int
    assert ta.may_trim(np.int32,    np.int64)     is False  # widening int
    assert ta.may_trim(np.int32,    np.float64)   is False  # int -> float (upgrade)
    assert ta.may_trim(np.dtype("<U10"), np.dtype("<U5")) is True   # shorten string
    assert ta.may_trim(np.dtype("<U5"),  np.dtype("<U10")) is False # lengthen string


# --- to_typed_array -----------------------------------------------------------
def test_to_typed_array():
    log = DummyLogger()

    # No-trim scenarios -> NO warnings
    # a) complex with zero imag -> float64 identical
    out = ta.to_typed_array(np.array([1+0j, 2+0j, -3+0j], dtype=np.complex128), "float64", log)
    assert out.dtype == np.float64
    assert np.array_equal(out, np.array([1.0, 2.0, -3.0]), equal_nan=False)
    assert log.count_level("WARNING") == 0

    # b) strings that fit the target length
    log.clear()
    out = ta.to_typed_array(["hi", "a", ""], "str[3]", log)
    assert out.dtype == np.dtype("<U3")
    assert out.tolist() == ["hi", "a", ""]
    assert log.count_level("WARNING") == 0

    # c) widening int
    log.clear()
    out = ta.to_typed_array(np.array([1, 2, 3], dtype=np.int32), "int64", log)
    assert out.dtype == np.int64
    assert log.count_level("WARNING") == 0

    # Trim scenarios -> WARNING with TrimmedArrayWarning instance captured
    # 1) float -> int (fractional parts)
    log.clear()
    out = ta.to_typed_array(np.array([1.2, -3.7, 4.0], dtype=np.float64), "int32", log)
    assert out.dtype == np.int32
    assert log.count_level("WARNING") >= 1
    assert log.has_warning_instance(ta.TrimmedArrayWarning)

    # 2) complex with non-zero imag -> float64 (imag dropped)
    log.clear()
    out = ta.to_typed_array(np.array([1+0j, 2+1j, -3+0j], dtype=np.complex128), "float64", log)
    assert out.dtype == np.float64
    assert log.count_level("WARNING") >= 1
    assert log.has_warning_instance(ta.TrimmedArrayWarning)

    # 3) string truncation (first element trims)
    log.clear()
    out = ta.to_typed_array(["abcdef", "xy", ""], "str[3]", log)
    assert out.dtype == np.dtype("<U3")
    assert out.tolist() == ["abc", "xy", ""]
    assert log.count_level("WARNING") >= 1
    assert log.has_warning_instance(ta.TrimmedArrayWarning)

    # 4) narrowing int (overflow/changes), e.g., int64 -> int8
    log.clear()
    src = np.array([0, 127, 128, -129], dtype=np.int64)
    out = ta.to_typed_array(src, "int8", log)
    assert out.dtype == np.int8
    # at least one value must differ after cast (128, -129)
    assert not np.array_equal(src, out.astype(src.dtype), equal_nan=False)
    assert log.count_level("WARNING") >= 1
    assert log.has_warning_instance(ta.TrimmedArrayWarning)
