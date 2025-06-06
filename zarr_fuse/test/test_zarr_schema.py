import pytest
from pathlib import Path

script_dir = Path(__file__).parent
inputs_dir = script_dir / "inputs"

from zarr_fuse import schema

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
    stream = schema.serialize(node_schema)
    struc2 = schema.deserialize(stream)
    assert schema.serialize(node_schema) == schema.serialize(struc2)

    schema.serialize(struc2, path=tmp_dir / struc_yaml)
    struc3 = schema.deserialize(tmp_dir / struc_yaml)
    assert schema.serialize(node_schema) == schema.serialize(struc3)

    return node_schema

@pytest.mark.parametrize("struc_yaml",
        ["structure_weather.yaml",
         "structure_tensors.yaml",
         "structure_tree.yaml"])
def test_schema_serialization(smart_tmp_path, struc_yaml):
    node_schema = aux_read_struc(smart_tmp_path, struc_yaml)
    fn_name = f"check_{(inputs_dir/struc_yaml).stem}"
    check_fn = globals()[fn_name]
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
    assert len(ds_schema.VARS) == 4, \
        f"Expected 1 variable definition, got {len(ds_schema.VARS)}"
    assert "temperature" in ds_schema.VARS, "Expected 'temperature' variable in VARS"

    # Print coordinate definitions.
    assert len(coords['time of year'].composed) == 1
    assert len(coords['lat_lon'].composed) == 2

    print("Coordinates:")
    for coord_name, coord_details in coords.items():
        print(f"{coord_name}: {coord_details}")

    # Print primary variable(s).
    print("\nQuantities:")
    for var_name, var_details in ds_schema.VARS.items():
        print(f"{var_name}: {var_details}")

def check_structure_tensors(structure):
    ds_schema = structure.ds
    assert isinstance(ds_schema, schema.DatasetSchema), \
        "Expected DatasetSchema instance"

    assert len(ds_schema.COORDS) == 3
    assert len(ds_schema.VARS) == 5
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
    ref_node = (["temperature", "time"], ["time"])

    children_0 = _check_node(structure, ref_node)
    children_1 = _check_node(children_0['child_1'], ref_node)
    _ = _check_node(children_1['child_3'], ref_node)
    assert len(_) == 0
    _ = _check_node(children_0['child_2'], ref_node)
    assert len(_) == 0