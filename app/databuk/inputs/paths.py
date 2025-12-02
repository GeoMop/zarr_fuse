import pathlib


# common output driectory
__script_dir__ = pathlib.Path(__file__).parent

# Following is public
input_dir = __script_dir__
schema_bukov_yaml = input_dir / "schemas" / "bukov_schema.yaml"
bukov_locations_csv = input_dir / "bukov_locations.csv"

# test data
test_dir = __script_dir__ / "test"
work_dir = test_dir / "workdir"
work_dir.mkdir(parents=True, exist_ok=True)

measurements_dir = test_dir / "test_measurements"
_measurements = ['20250915T111522_824a7f3dc0ad.json', '20250915T115149_8b4f1f4535aa.json',
'20250915T133948_121e738c86ab.json', 'T_123_partial.json']
old_measurements = [measurements_dir / _measurements[-1]]
new_measurements = [measurements_dir / _m for _m in _measurements[:-1]]

