import pytest
import warnings
import logging
import pint
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


def test_schemaerror():
    assert issubclass(schema.SchemaError, Exception)

    with pytest.raises(schema.SchemaError) as ei:
        raise schema.SchemaError("boom", schema.SchemaCtx(["x", "y"], file="f.yaml"))
    s = str(ei.value)
    assert "boom" in s
    assert "(at f.yaml:x/y)" in s

def test_schemawarning():
    assert issubclass(schema.SchemaWarning, UserWarning)
    assert issubclass(schema.SchemaWarning, Warning)

    warn_obj = schema.SchemaWarning("heads up", schema.SchemaCtx(["x"], file="f.yaml"))
    # __str__ should work
    s = str(warn_obj)
    assert "heads up" in s and "f.yaml:x" in s

    # Emitting via warnings.warn should capture our custom warning
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        warnings.warn(warn_obj)
        msgs = [w.message for w in rec]
        assert any(isinstance(m, schema.SchemaWarning) for m in msgs)


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


@pytest.mark.parametrize("struc_yaml",
        ["structure_weather.yaml",
         "structure_tensors.yaml",
         "structure_tree.yaml"])
def test_schema_serialization(smart_tmp_path, struc_yaml):
    node_schema = aux_read_struc(smart_tmp_path, struc_yaml)
    fn_name = f"check_{(inputs_dir/struc_yaml).stem}"
    check_fn = globals()[fn_name]

    print(f"Checking function {fn_name}...")
    check_fn(node_schema)


def check_structure_weather(node_schema):
    ds_schema = node_schema.ds
    assert isinstance(ds_schema, schema.DatasetSchema), "Expected DatasetSchema instance"
    coords = ds_schema.COORDS

    # Check that COORDS is a dictionary with 2 entries.
    assert isinstance(coords, dict), "COORDS must be a dictionary"
    assert len(coords) == 2, \
        f"Expected 2 coordinate definitions, got {len(ds_schema.COORDS)}"

    # Check that VARS is a dictionary and that only one primary variable remains.
    # In our processed structure, we expect that variables used only as coordinates
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

    assert ds_schema.COORDS["time of year"].step_limits == schema.Interval(1, 1, units.Unit('h'))

    # Print primary variable(s).
    print("\nQuantities:")
    for var_name, var_details in ds_schema.VARS.items():
        print(f"{var_name}: {var_details}")


def check_structure_tensors(structure):
    ds_schema = structure.ds
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


def check_structure_tree(structure):
    ref_node = (["temperature"], ["time"])

    children_0 = _check_node(structure, ref_node)
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

    # attrs expose expected keys
    v, _ = _mk_var({"name": "v", "coords": []})
    assert {"unit", "description", "df_col", "source_unit"} <= set(v.attrs.keys())

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
    v, _ = _mk_var({"name": "v", "coords": [], "unit": None, "range": {"discrete": [1, 2, 3]}})
    assert isinstance(v.range, schema.DiscreteRange)
    assert list(v.range.values) == [1, 2, 3]

    # conversion source_unit → unit
    #v, _ = _mk_var({"name": "v", "coords": [], "unit": "m", "source_unit": "cm"})
    #q = v.convert_values([0, 100])
    #assert str(q.units) in {"meter", "m"} and np.allclose(q.m, [0.0, 1.0])


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

    # step_limits default interval vs. explicit None
    c, _ = _mk_coord({"name": "t", "coords": []})
    assert c.step_limits == schema.Interval(None, None, pint.Unit(""))
    c, _ = _mk_coord({"name": "t", "coords": [], "step_limits": None})
    assert c.step_limits is None

    # sorted default depends on composition
    c, _ = _mk_coord({"name": "x", "coords": []})
    assert c.sorted is True
    c, _ = _mk_coord({"name": "xy", "coords": [], "composed": ["x", "y"]})
    assert c.is_composed() and c.sorted is False