import pathlib

# common output driectory
__script_dir__ = pathlib.Path(__file__).parent
work_dir = __script_dir__.parent / "workdir"
work_dir.mkdir(parents=True, exist_ok=True)

# Following is public
input_dir = __script_dir__.parent / "inputs"
schema_bukov_yaml = input_dir / "schema_bukov.yaml"
bukov_locations_csv = input_dir / "bukov_locations.csv"
