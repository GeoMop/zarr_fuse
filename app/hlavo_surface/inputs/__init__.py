import pathlib

# common output driectory
__script_dir__ = pathlib.Path(__file__).parent
work_dir = __script_dir__.parent / "workdir"
work_dir.mkdir(parents=True, exist_ok=True)

# Following is public
input_dir = __script_dir__.parent / "inputs"
surface_schema_yaml = input_dir / "surface_schema.yaml"
odyssey_locations_csv = input_dir / "odyssey_locations.csv"
