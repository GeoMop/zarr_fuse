import pytest
import warnings
from zarr_fuse import schema_ctx as zf_ctx

def test_schemaerror():
    assert issubclass(zf_ctx.SchemaError, Exception)

    with pytest.raises(zf_ctx.SchemaError) as ei:
        raise zf_ctx.SchemaError("boom", zf_ctx.SchemaCtx(["x", "y"], file="f.yaml"))
    s = str(ei.value)
    assert "boom" in s
    assert "(at f.yaml:x/y)" in s

def test_schemawarning():
    assert issubclass(zf_ctx.SchemaWarning, UserWarning)
    assert issubclass(zf_ctx.SchemaWarning, Warning)

    warn_obj = zf_ctx.SchemaWarning("heads up", zf_ctx.SchemaCtx(["x"], file="f.yaml"))
    # __str__ should work
    s = str(warn_obj)
    assert "heads up" in s and "f.yaml:x" in s

    # Emitting via warnings.warn should capture our custom warning
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        warnings.warn(warn_obj)
        msgs = [w.message for w in rec]
        assert any(isinstance(m, zf_ctx.SchemaWarning) for m in msgs)

