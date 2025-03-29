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
    structure = schema.deserialize(struc_path)
    assert set(['COORDS', 'VARS', 'ATTRS']).issubset(set(structure.keys()))
    stream = schema.serialize(structure)
    struc2 = schema.deserialize(stream)
    assert schema.serialize(structure) == schema.serialize(struc2)

    schema.serialize(struc2, path=tmp_dir / struc_yaml)
    struc3 = schema.deserialize(tmp_dir / struc_yaml)
    assert schema.serialize(structure) == schema.serialize(struc3)

    return structure

@pytest.mark.parametrize("struc_yaml",
        ["structure_weather.yaml",
         "structure_tensors.yaml",
         "structure_tree.yaml"])
def test_schema_serialization(smart_tmp_path, struc_yaml):
    struc = aux_read_struc(smart_tmp_path, struc_yaml)
    fn_name = f"check_{(inputs_dir/struc_yaml).stem}"
    check_fn = globals()[fn_name]
    check_fn(struc)

def check_structure_weather(structure):
    # Check that COORDS is a dictionary with 2 entries.
    assert isinstance(structure.get('COORDS'), dict), "COORDS must be a dictionary"
    assert len(structure['COORDS']) == 2, f"Expected 2 coordinate definitions, got {len(structure['COORDS'])}"

    # Check that VARS is a dictionary and that only one primary variable remains.
    # In our processed structure, we expect that variables used only as coordinates
    # (e.g., "time of year", "latitude", "longitude") are removed and only "temperature" remains.
    assert isinstance(structure.get('VARS'), dict), "VARS must be a dictionary"
    assert len(structure['VARS']) == 4, f"Expected 1 variable definition, got {len(structure['VARS'])}"
    assert "temperature" in structure['VARS'], "Expected 'temperature' variable in VARS"

    # Print coordinate definitions.
    coords = structure["COORDS"]
    assert len(coords['time of year'].composed) == 1
    assert len(coords['lat_lon'].composed) == 2

    print("Coordinates:")
    for coord_name, coord_details in structure["COORDS"].items():
        print(f"{coord_name}: {coord_details}")

    # Print primary variable(s).
    print("\nQuantities:")
    for var_name, var_details in structure["VARS"].items():
        print(f"{var_name}: {var_details}")

def check_structure_tensors(structure):
    assert len(structure['COORDS']) == 3
    assert len(structure['VARS']) == 5
    print("Coordinates:")
    for coord in structure["COORDS"]:
        print(coord)
    print("\nQuantities:")
    for var in structure["VARS"]:
        print(var)

def _check_node(struc, ref_node):
    vars, coords = ref_node
    assert set(struc['VARS'].keys()) == set(vars)
    assert set(struc['COORDS'].keys()) == set(coords)

def check_structure_tree(structure):
    ref_node = (["temperature", "time"], ["time"])

    _check_node(structure, ref_node)
    _check_node(structure['child_1'], ref_node)
    _check_node(structure['child_1']['child_3'], ref_node)
    _check_node(structure['child_2'], ref_node)