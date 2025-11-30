# zarr_fuse/test/test_dtype_converter.py
import numpy as np
import pytest

import zarr_fuse.dtype_converter as ta


# --- Minimal context + config shims to drive DType.from_cfg -------------------
class DummyCtx:
    """Mimics zarr_fuse.schema_ctx.SchemaCtx just enough for tests."""
    def __init__(self, name="dummy"):
        self.name = name
        self.records = []  # list of tuples: (level, obj)

    def warning(self, obj, *args, **kwargs):
        self.records.append(("WARNING", obj))

    def error(self, obj, *args, **kwargs):
        self.records.append(("ERROR", obj))

    # helpers
    def count_level(self, level: str) -> int:
        return sum(1 for lvl, _ in self.records if lvl == level)

    def has_warning_instance(self, cls) -> bool:
        return any(lvl == "WARNING" and isinstance(obj, cls) for lvl, obj in self.records)

    def clear(self):
        self.records.clear()


class DummyCfg:
    """Mimics zarr_fuse.schema_ctx.ContextCfg(value, schema_ctx)."""
    def __init__(self, value, schema_ctx: DummyCtx):
        self._value = value
        self.schema_ctx = schema_ctx

    def value(self):
        return self._value


# --- DType.from_cfg (replaces old _parse_dtype_spec tests) --------------------
def test_dtype_from_cfg_basic_and_str():
    ctx = DummyCtx()

    # None -> dtype(None)
    dt = ta.DType.from_cfg(DummyCfg(None, ctx)).dtype
    assert dt is None
    assert ctx.count_level("ERROR") == 0

    # Valid basic specs (mapped by your new type_mapping)
    basics = [
        ("int",   np.int64),
        ("int8",  np.int8),
        ("int32", np.int32),
        ("int64", np.int64),
        ("uint8", np.uint8),
        ("uint32", np.uint32),
        ("uint64", np.uint64),
        ("float", np.float64),
        ("float32", np.float32),
        ("float64", np.float64),
        ("complex", np.complex128),
        ("complex64", np.complex64),
        ("complex128", np.complex128),
    ]
    for spec, expected in basics:
        dt = ta.DType.from_cfg(DummyCfg(spec, ctx)).dtype
        assert dt == np.dtype(expected)
    assert ctx.count_level("ERROR") == 0

    # str[n]
    ctx.clear()
    dt = ta.DType.from_cfg(DummyCfg("str[5]", ctx)).dtype
    assert dt == np.dtype("<U5")
    assert ctx.count_level("ERROR") == 0

    # bare 'str' -> default length warning to <U32
    ctx.clear()
    dt = ta.DType.from_cfg(DummyCfg("str", ctx)).dtype
    assert dt == np.dtype("<U32")
    assert ctx.count_level("WARNING") >= 1

    # str[0] -> warning + default length <U32
    ctx.clear()
    dt = ta.DType.from_cfg(DummyCfg("str[0]", ctx)).dtype
    assert dt == np.dtype("<U32")
    assert ctx.count_level("WARNING") >= 1

    # unsupported -> error recorded
    ctx.clear()
    _ = ta.DType.from_cfg(DummyCfg("nope", ctx)).dtype
    assert ctx.count_level("ERROR") >= 1


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
    assert ta.may_trim(np.dtype("<U10"), np.dtype("<U5")) is True   # shorten string
    assert ta.may_trim(np.dtype("<U5"),  np.dtype("<U10")) is False # lengthen string


# --- to_typed_array -----------------------------------------------------------
def test_to_typed_array():
    ctx = DummyCtx()

    # No-trim scenarios -> NO warnings
    # a) complex with zero imag -> float64 identical
    out = ta.to_typed_array(np.array([1+0j, 2+0j, -3+0j], dtype=np.complex128), np.float64, ctx)
    assert out.dtype == np.float64
    assert np.array_equal(out, np.array([1.0, 2.0, -3.0]), equal_nan=False)
    assert ctx.count_level("WARNING") == 0

    # b) strings that fit the target length
    ctx.clear()
    out = ta.to_typed_array(["hi", "a", ""], np.dtype("<U3"), ctx)
    assert out.dtype == np.dtype("<U3")
    assert out.tolist() == ["hi", "a", ""]
    assert ctx.count_level("WARNING") == 0

    # c) widening int
    ctx.clear()
    out = ta.to_typed_array(np.array([1, 2, 3], dtype=np.int32), np.int64, ctx)
    assert out.dtype == np.int64
    assert ctx.count_level("WARNING") == 0

    # Trim scenarios -> WARNING with TrimmedArrayWarning instance captured
    # 1) float -> int (fractional parts)
    ctx.clear()
    out = ta.to_typed_array(np.array([1.2, -3.7, 4.0], dtype=np.float64), np.int32, ctx)
    assert out.dtype == np.int32
    assert ctx.count_level("WARNING") >= 1
    assert ctx.has_warning_instance(ta.TrimmedArrayWarning)

    # 2) complex with non-zero imag -> float64 (imag dropped)
    ctx.clear()
    out = ta.to_typed_array(np.array([1+0j, 2+1j, -3+0j], dtype=np.complex128), np.float64, ctx)
    assert out.dtype == np.float64
    assert ctx.count_level("WARNING") >= 1
    assert ctx.has_warning_instance(ta.TrimmedArrayWarning)

    # 3) string truncation (first element trims)
    ctx.clear()
    out = ta.to_typed_array(["abcdef", "xy", ""], np.dtype("<U3"), ctx)
    assert out.dtype == np.dtype("<U3")
    assert out.tolist() == ["abc", "xy", ""]
    assert ctx.count_level("WARNING") >= 1
    assert ctx.has_warning_instance(ta.TrimmedArrayWarning)

    # 4) narrowing int (overflow/changes), e.g., int64 -> int8
    ctx.clear()
    src = np.array([0, 127, 128, -129], dtype=np.int64)
    out = ta.to_typed_array(src, np.int8, ctx)
    assert out.dtype == np.int8
    # at least one value must differ after cast (128, -129)
    assert not np.array_equal(src, out.astype(src.dtype), equal_nan=False)
    assert ctx.count_level("WARNING") >= 1
    assert ctx.has_warning_instance(ta.TrimmedArrayWarning)
