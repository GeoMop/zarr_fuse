import pytest
import warnings
import logging
#import pint
from zarr_fuse import schema, units
import numpy as np

from pathlib import Path
script_dir = Path(__file__).parent
inputs_dir = script_dir / "inputs"


def test_default_logger_error_and_logging(caplog, attach_logger):
    logger = schema.default_logger()
    attach_logger(logger)


    # Case: Exception instance → logs then raises same instance
    with pytest.raises(ValueError) as ei2:
        logger.error(ValueError("bad-value"))
    assert isinstance(ei2.value, ValueError)
    assert "bad-value" in str(ei2.value)
    assert any(
        rec.levelno == logging.ERROR and "bad-value" in rec.getMessage()
        for rec in caplog.records
    )

    caplog.clear()
    # Case: plain message → logs ERROR then raises RuntimeError
    with pytest.raises(RuntimeError) as ei:
        logger.error("boom-message")
    # Check exception message
    assert "boom-message" in str(ei.value)

    # Check that the log record was produced
    # `caplog.records` is a list of LogRecord objects
    assert any(
        rec.levelno == logging.ERROR and "boom-message" in rec.getMessage()
        for rec in caplog.records
    )

    caplog.clear()




def test_schemaaddress():
    a = schema.SchemaCtx(["root", "section", "key"], file="cfg.yaml")
    assert str(a) == "cfg.yaml:root/section/key"

    b = schema.SchemaCtx(["only"])
    assert str(b) == "<SCHEMA STREAM>:only"


def test_schemaaddress_dive_and_int_key():
    base = schema.SchemaCtx(["root"], file="cfg.yaml")
    a = base.dive("VARS", "temp", 0)
    assert str(a) == "cfg.yaml:root/VARS/temp/0"


def _mk_var_for_convert(**kwargs):
    """
    Helper to build a Variable instance for convert_values / convert_value tests.

    kwargs are the raw schema fields (without ContextCfg), e.g.:

        _mk_var_for_convert(
            name="height",
            coords=[],
            unit="m",
            source_unit="cm",
            type="float64",
            range={"interval": [0, 2]},
        )
    """
    base = {"name": "height", "coords": []}
    base.update(kwargs)
    v, _ = _mk_var(base)
    return v

def test_variable():
    # --- setup: numeric variable with unit conversion + interval range ---
    v = _mk_var_for_convert(
        unit="m",
        source_unit="cm",
        type="float64",
        range={"interval": [0, 2]},  # allowed range in *meters*
    )

    # Basic sanity: dtype and units
    assert isinstance(v.unit, units.Unit)
    assert v.unit == units.Unit("m")
    assert v.source_unit == units.Unit("cm")
    assert v.dtype == np.dtype("float64")

    # --- convert_values: default from_unit/source_unit and range enforcement ---

    # Values are in centimeters (source_unit); they should be converted to meters.
    vals_cm = np.array([0.0, 50.0, 100.0])  # cm
    converted = v.convert_values(vals_cm)
    # 0 cm -> 0 m, 50 cm -> 0.5 m, 100 cm -> 1 m
    assert np.allclose(converted, np.array([0.0, 0.5, 1.0], dtype=np.float64))

    # Values that land outside [0, 2] meters should raise a ValueError
    vals_cm_out_of_range = np.array([-10.0, 50.0])  # -> [-0.1, 0.5] m
    with pytest.raises(ValueError):
        v.convert_values(vals_cm_out_of_range)

    # --- convert_values: overriding units and range ---

    # Supply values in millimeters but override from_unit/to_unit + range.
    vals_mm = np.array([0.0, 500.0, 1000.0])  # mm
    custom_range = schema.InfRange()  # disables range checking

    converted_mm = v.convert_values(
        vals_mm,
        from_unit=units.Unit("mm"),
        to_unit=units.Unit("m"),
        range=custom_range,
    )
    # 0 mm -> 0 m, 500 mm -> 0.5 m, 1000 mm -> 1 m
    assert np.allclose(converted_mm, np.array([0.0, 0.5, 1.0], dtype=np.float64))

    # --- encode / decode + quantity / magnitude helpers ---

    # encode should run through v.range.encode and be identity within range
    values_m = np.array([0.25, 0.75], dtype=np.float64)
    encoded = v.encode(values_m)
    assert np.allclose(encoded, values_m)

    # decode should give back a Quantity in the variable's unit
    decoded_q = v.decode(encoded)
    assert isinstance(decoded_q, units.Quantity)
    assert decoded_q.unit == v.unit
    assert np.allclose(v.magnitude(decoded_q), values_m)

    # quantity() given raw array should wrap it into a Quantity in v.unit
    q_from_raw = v.quantity(values_m)
    assert isinstance(q_from_raw, units.Quantity)
    assert q_from_raw.units == v.unit
    assert np.allclose(q_from_raw.magnitude, values_m)

    # magnitude() on a plain ndarray should return it unchanged
    assert np.allclose(v.magnitude(values_m), values_m)

    # --- schema.convert_value applied to Variable instance ---

    serialized = schema.convert_value(v)
    # Must be a plain dict, no internal ContextCfg / SchemaCtx leaking out
    assert isinstance(serialized, dict)
    assert serialized["name"] == "height"
    assert serialized["coords"] == []
    assert "_address" not in serialized

    # Type should round-trip into some string representation
    # (exact token is tested elsewhere; here we only care that it's serializable)
    assert isinstance(serialized["type"], str)

    # Range should be serialized via IntervalRange.asdict -> {"interval": [start, end, unit_like]}
    assert "range" in serialized
    assert isinstance(serialized["range"], dict)
    assert "interval" in serialized["range"]
    interval_spec = serialized["range"]["interval"]
    assert interval_spec[0] == 0
    assert interval_spec[1] == 2
    # Don't pin down the exact representation of the unit; just ensure it's there.
    assert len(interval_spec) == 3

    v = _mk_var_for_convert(
        unit={ "tick": "s", "tz": "UTC" },
        # range={"interval": [0, 2]},  # TODO: test data time ranges
    )

    values = np.array(
        [
            "2023-01-01T00:00:00",
            "2022-10-05T16:00:00+00:00",
            #"",  # test missing value handling
        ])

    converted = v.convert_values(values)

    # We expect a proper numpy array back, not None.
    assert converted.dtype == np.dtype('datetime64[s]')
    assert isinstance(converted, np.ndarray)
    assert converted.shape == (2,)

    # No element should be None (this is the regression we want to guard).
    assert all(x is not None for x in converted)

    # Sanity: ordering should be preserved – 2022-10-05 < 2023-01-01
    # regardless of how the underlying DateTimeQuantity is represented.
    assert converted[1] < converted[0]

CASES = [
    # # integers
    # (np.int64, -9999, 1),
    # (np.int32, -1, 0),
    #
    # # floats (numeric sentinel)
    # (np.float64, -9999.0, 0.0),
    # (np.float32, -1.0, 1.5),
    #
    # # floats (NaN sentinel)
    # (np.float64, np.nan, 0.0),
    # (np.float32, np.float32("nan"), np.float32(1.0)),
    #
    # # complex
    # (np.complex128, complex(0.0, 0.0), complex(1.0, -1.0)),
    # (np.complex64, np.complex64(0 + 0j), np.complex64(2 + 3j)),
    #
    # # strings
    # ("U10", "missing", "ok"),
    # ("U5", "", "val"),

    # datetime64 (NaT sentinel)
    ("datetime64[ns]", np.datetime64("NaT", "ns"), np.datetime64("2020-01-01T00:00:00")),
    ("datetime64[D]",  np.datetime64("NaT", "D"),  np.datetime64("2020-01-01", "D")),
]

@pytest.mark.parametrize("dtype, sentinel, valid_value", CASES)
def test_variable_mask_valid(dtype, sentinel, valid_value):
    # Build the test array [sentinel, valid_value] with the given dtype
    arr = np.array([sentinel, valid_value], dtype=dtype)

    var = _mk_var_for_convert(
        na_value=sentinel,
        type=schema.DType(np.dtype(dtype)).asdict(None, None)
    )
    mask = var.valid_mask(arr)

    # By definition: sentinel is invalid, valid_value is valid
    expected = np.array([False, True], dtype=bool)

    assert mask.shape == (2,)
    assert mask.dtype == bool
    np.testing.assert_array_equal(mask, expected)




"""
This is an inital test of xarray, zarr functionality that we build on.
This requires dask.
"""

def aux_read_struc(tmp_dir, struc_yaml):
    struc_path = inputs_dir / struc_yaml
    node_schema = schema.deserialize(struc_path)
    assert isinstance(node_schema, schema.NodeSchema)
    assert isinstance(node_schema.groups, dict)
    assert isinstance(node_schema.ds, schema.DatasetSchema)
    stream_1 = schema.serialize(node_schema)
    schema_2 = schema.deserialize(stream_1)

    schema.serialize(schema_2, path=tmp_dir / struc_yaml)
    stream_2 = schema.serialize(schema_2)

    # Stability of second serialization
    assert stream_1 == stream_2, "Second serialization mismatch"
    # Stability of second deserialization
    assert node_schema == schema_2

    return node_schema


@pytest.mark.parametrize(
    "struc_yaml",
    ["schema_weather.yaml", "schema_tensors.yaml", "schema_tree.yaml"],
)
def test_schema_serialization(smart_tmp_path, struc_yaml):
    node_schema = aux_read_struc(smart_tmp_path, struc_yaml)
    fn_name = f"check_{(inputs_dir/struc_yaml).stem}"
    check_fn = globals()[fn_name]

    print(f"Checking function {fn_name}...")
    check_fn(node_schema)


def check_schema_weather(node_schema):
    ds_schema = node_schema.ds
    assert isinstance(ds_schema, schema.DatasetSchema), "Expected DatasetSchema instance"
    coords = ds_schema.COORDS

    # Check that COORDS is a dictionary with 2 entries.
    assert isinstance(coords, dict), "COORDS must be a dictionary"
    assert len(coords) == 2, \
        f"Expected 2 coordinate definitions, got {len(ds_schema.COORDS)}"

    # Check that VARS is a dictionary and that only one primary variable remains.
    # In our processed schema, we expect that variables used only as coordinates
    # (e.g., "time of year", "latitude", "longitude") are removed and only "temperature" remains.
    assert isinstance(ds_schema.VARS, dict), "VARS must be a dictionary"
    assert len(ds_schema.VARS) == 3, \
        f"Expected 3 variable definition, got {len(ds_schema.VARS)}"
    assert "temperature" in ds_schema.VARS, "Expected 'temperature' variable in VARS"

    # Print coordinate definitions.
    assert len(coords['time of year'].composed) == 1
    assert len(coords['lat_lon'].composed) == 2

    print("Coordinates:")
    for coord_name, coord_details in coords.items():
        print(f"{coord_name}: {coord_details}")

    assert ds_schema.COORDS["time of year"].step_limits == schema.Interval(1, 12, units.Unit('h'))

    # Print primary variable(s).
    print("\nQuantities:")
    for var_name, var_details in ds_schema.VARS.items():
        print(f"{var_name}: {var_details}")


def check_schema_tensors(schema_tn):
    ds_schema = schema_tn.ds
    assert isinstance(ds_schema, schema.DatasetSchema), \
        "Expected DatasetSchema instance"

    assert len(ds_schema.COORDS) == 3
    assert len(ds_schema.VARS) == 2
    print("Coordinates:")
    for coord in ds_schema.COORDS:
        print(coord)
    print("\nQuantities:")
    for var in ds_schema.VARS:
        print(var)


def _check_node(struc, ref_node):
    ds_schema = struc.ds
    assert isinstance(ds_schema, schema.DatasetSchema), "Expected DatasetSchema instance"

    vars, coords = ref_node
    assert set(ds_schema.VARS.keys()) == set(vars)
    assert set(ds_schema.COORDS.keys()) == set(coords)
    return struc.groups


def check_schema_tree(schema_tree):
    ref_node = (["temperature"], ["time"])

    children_0 = _check_node(schema_tree, ref_node)
    children_1 = _check_node(children_0['child_1'], ref_node)
    _ = _check_node(children_1['child_3'], ref_node)
    assert len(_) == 0
    _ = _check_node(children_0['child_2'], ref_node)
    assert len(_) == 0


# -------------------- new tests for _address + helpers -------------------- #


class _TestLogger:
    def __init__(self):
        self.errors = []
        self.warnings = []
    def error(self, exc, *args, **kwargs):
        self.errors.append(str(exc))
    def warning(self, warn, *args, **kwargs):
        self.warnings.append(str(warn))

def _ctx(data: dict, *, logger=None, path=None):
    """Create a ContextCfg with a SchemaCtx wired to our capture logger."""
    logger = logger or _TestLogger()
    ctx = schema.SchemaCtx(addr=[] if path is None else path, file="test.yaml", logger=logger)
    return schema.ContextCfg(data, ctx), logger

def _mk_var(d: dict, *, logger=None):
    cfg, log = _ctx(d, logger=logger)
    return schema.Variable(cfg), log

def _mk_coord(d: dict, *, logger=None):
    cfg, log = _ctx(d, logger=logger)
    return schema.Coord(cfg), log

def _has(bag: list[str], needle: str) -> bool:
    return any(needle in s for s in bag)

@pytest.mark.parametrize(
    "token, expected_dtype, expected_str",
    [
        ("bool", np.dtype("int8"), "int8"),
        ("int", np.dtype("int64"), "int64"),
        ("int8", np.dtype("int8"), "int8"),
        ("int32", np.dtype("int32"), "int32"),
        ("int64", np.dtype("int64"), "int64"),
        ("uint", np.dtype("uint64"), "uint64"),
        ("uint64", np.dtype("uint64"), "uint64"),
        ("float", np.dtype("float64"), "float64"),
        ("float32", np.dtype("float32"), "float32"),
        ("float64", np.dtype("float64"), "float64"),
        ("complex", np.dtype("complex128"), "complex128"),
        ("complex64", np.dtype("complex64"), "complex64"),
        ("complex128", np.dtype("complex128"), "complex128"),
        ("str[7]", np.dtype("<U7"), "str[7]"),
    ],
)
def test_dtype_parse_and_serialize_roundtrip(token, expected_dtype, expected_str):
    # Build cfg = ContextCfg(str) with a test logger so errors (if any) are captured but won't raise.
    test_logger = _TestLogger()
    cfg = schema.ContextCfg(token, schema.SchemaCtx(["VARS", "x", "type"], file="cfg.yaml", logger=test_logger))

    # Deserialize
    dt = schema.DType.from_cfg(cfg)
    assert dt.dtype == expected_dtype

    # Serialize back
    s = dt.asdict(None, None)
    assert s == expected_str


def test_variable_logging_and_basics():
    # logs error on invalid unit
    _, log = _mk_var({"name": "v", "coords": [], "unit": "NOT_A_UNIT"})
    assert _has(log.errors, "Invalid unit string")

    # error on invalid source_unit
    _, log = _mk_var({"name": "v", "coords": [], "unit": "m", "source_unit": "NOPE"})
    assert _has(log.errors, "Invalid unit string")

    # error on invalid range key
    _, log = _mk_var({"name": "v", "coords": [], "range": {"bogus": 123}})
    assert _has(log.errors, "Invalid range specification")

    # coords: str -> list normalization (sanity check)
    v, _ = _mk_var({"name": "v", "coords": "x"})
    assert v.coords == ["x"]

    # attrs exist on the object and serialize via zarr_attrs()
    v, _ = _mk_var({"name": "v", "coords": []})
    assert hasattr(v, "unit") and hasattr(v, "description") and hasattr(v, "df_col") and hasattr(v, "source_unit")
    za = v.zarr_attrs()
    assert {"unit", "description", "df_col", "source_unit"}.issubset(set(za.keys()))

    # warning path through logger
    v, log = _mk_var({"name": "v", "coords": []})
    v.warn("be careful")
    assert _has(log.warnings, "be careful")

    # ---------- NEW unit & range initialization checks ----------

    # unit + source_unit parsing
    v, _ = _mk_var({"name": "v", "coords": [], "unit": "m", "source_unit": "cm"})
    assert v.unit == schema.units.Unit("m") and v.source_unit == schema.units.Unit("cm")

    # interval range: default unit == variable.unit
    v, _ = _mk_var({"name": "v", "coords": [], "unit": "m", "range": {"interval": [0, 10]}})
    assert isinstance(v.range, schema.IntervalRange)
    assert (v.range.start, v.range.end) == (0, 10) and v.range.unit == v.unit

    # interval range: explicit unit in list overrides
    v, _ = _mk_var({"name": "v", "coords": [], "unit": "m", "range": {"interval": [0, 100, "cm"]}})
    assert isinstance(v.range, schema.IntervalRange)
    assert v.range.unit == schema.units.Unit("cm")

    # discrete range
    v, _ = _mk_var({
        "name": "v",
        "coords": [],
        "unit": None,
        "type": "int64",
        "na_value": -1,
        "range": {"discrete": [1, 2, 3]}})
    assert isinstance(v.range, schema.DiscreteRange)
    assert list(v.range.codes_to_labels) == [-1, 1, 2, 3]


def test_coord_specific_attributes():
    # default coords injected == name
    c, _ = _mk_coord({"name": "time"})
    assert c.coords == ["time"]

    # default composed is singleton name
    c, _ = _mk_coord({"name": "y", "coords": []})
    assert c.composed == ["y"]

    # explicit composed respected
    c, _ = _mk_coord({"name": "yx", "coords": [], "composed": ["y", "x"]})
    assert c.composed == ["y", "x"]

    # chunk_size default and custom
    c, _ = _mk_coord({"name": "lat", "coords": []})
    assert c.chunk_size == 1024
    c, _ = _mk_coord({"name": "lat", "coords": [], "chunk_size": 64})
    assert c.chunk_size == 64

    # step_limits default and explicit None (match new semantics)
    # missing -> "any_new" => [-inf, +inf]
    c, _ = _mk_coord({"name": "t", "coords": []})   # default: "any_new"
    assert c.step_limits == schema.Interval(-np.inf, np.inf, units.Unit(""))

    # explicit None -> "no_new" => [NaN, NaN]
    c, _ = _mk_coord({"name": "t", "coords": [], "step_limits": "no_new"})
    assert c.step_limits == schema.Interval(-np.inf, -np.inf, units.Unit(""))

    # sorted default depends on composition
    c, _ = _mk_coord({"name": "x", "coords": []})
    assert c.sorted is True
    c, _ = _mk_coord({"name": "xy", "coords": [], "composed": ["x", "y"]})
    assert c.is_composed() and c.sorted is False


def test_discrete_range_roundtrip_and_encode_decode(tmp_path):
    # Prepare a ContextCfg that holds a simple list of labels (non-CSV path).
    labels = ["red", "green", "blue"]
    na = "<NA>"

    ctx = schema.SchemaCtx(["VARS", "color", "range"], file=str(tmp_path / "cfg.yaml"))
    cfg = schema.ContextCfg(labels, ctx)

    # Build DiscreteRange from cfg (list path); source_col/convert_fn are unused here
    dr = schema.DiscreteRange.from_cfg(cfg, source_col="color", convert_fn=lambda s: s, na_value=na)

    # The constructed table should have NA in slot 0, followed by labels
    assert list(dr.codes_to_labels) == [na, *labels]
    assert dr.na_value == na

    # Serialize to dict and ensure NA is omitted in the payload
    serialized = dr.asdict(None, None)
    assert serialized == {"discrete": labels}

    # Deserialize by feeding the serialized payload back through ContextCfg + from_cfg
    cfg2 = schema.ContextCfg(serialized["discrete"], ctx)
    dr2 = schema.DiscreteRange.from_cfg(cfg2, source_col="color", convert_fn=lambda s: s, na_value=na)

    # Roundtrip equality of the codes_to_labels array
    assert np.array_equal(dr2.codes_to_labels, dr.codes_to_labels)

    # Encode known + unknown values; unknown should map to code 0 (NA)
    to_encode = [labels[0], labels[1], "unknown", labels[2], na]
    encoded = dr2.encode(to_encode)
    assert np.array_equal(encoded, np.array([1, 2, 0, 3, 0], dtype=np.int64))

    # Decode should recover original labels with NA where appropriate
    decoded = dr2.decode(encoded)
    assert decoded.tolist() == [labels[0], labels[1], na, labels[2], na]
