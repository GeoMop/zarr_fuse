import pathlib

# common output driectory
__script_dir__ = pathlib.Path(__file__).parent
work_dir = __script_dir__.parent / "workdir"
work_dir.mkdir(parents=True, exist_ok=True)

# Following is public
input_dir = __script_dir__.parent / "input_data"
bh_cfg_yaml = input_dir / "boreholes.yaml"
piezo_filter_yaml = input_dir / "piezo_filtering.yaml"
events_yaml = input_dir / "events.yaml"
#blast_events_xlsx = input_dir / 'blast_events.xlsx'

# smallest file
piezo_measurement_file = input_dir / "piezo_2024_06_04.xlsx"
# last file
#piezo_measurement_file = input_dir / "piezo_2025_04_08.xlsx"
# configuration of the filtering sections
# named sections of the data corresponding to interesting time intervals
