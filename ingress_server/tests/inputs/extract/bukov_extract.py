import json
import re
import math
#import csv
import polars as pl
from datetime import datetime, timezone
from pathlib import Path

# IN_JSON  = Path("inputs/test_measurements/T_123_partial.json")            # input JSON with old data
# OUT_CSV  = Path("inputs/test_measurements/T_123_partial_normalized.csv")   # CSV output
#
# files = [
#     "20250915T133948_121e738c86ab.json",
#     "20250915T111522_824a7f3dc0ad.json",
#     "20250915T115149_8b4f1f4535aa.json",
# ]
#
# IN_JSON_NEW  = Path("inputs/test_measurements/20250915T111522_824a7f3dc0ad.json")          # input JSON from Fiedler
# OUT_CSV_NEW  = Path("inputs/test_measurements/from_fiedler_normalized.csv")   # CSV output

# Field names in source JSON, which will be renamed acc. to yaml
DATETIME_KEY  = "date"
AIR_TEMP_KEY  = "air_temp"
AIR_HUM_KEY   = "air_humidity"

# Depth keys considered rock temperatures: numbers+`m` (e.g. 0.05m, 0.5m, 1m, ... 10m) and 15mA/15mB
DEPTH_KEY_PATTERN = re.compile(r"""^(
    \d+(?:\.\d+)?m   # e.g. 0.05m, 0.5m, 1m, 2m, 10m
    |15m[AB]         # 15mA, 15mB
)$""", re.X)

def as_float_or_nan(x):
    """Convert to float when possible; otherwise NaN."""
    try:
        return float(x)
    except Exception:
        return math.nan

def depth_label(key: str) -> str:
    if key == "15mA": # convert '15mA' -> '15.1'
        return "15.1"
    if key == "15mB": # convert '15mB' -> '15.1'
        return "15.2"
    return key[:-1] if key.endswith("m") else key  # '0.05m' -> '0.05' etc.

# Maps new-format borehole keys (from Fiedler JSON) to canonical CSV keys
GPS_KEY_ALIASES = {
    'ZK-1_S4': 'ZK1-S4',
}

def normalize(json_dict, gps_dict):
    data = json_dict
    records = []

    for borehole, measurements in data.items():
        csv_key = GPS_KEY_ALIASES.get(borehole, borehole)
        borehole_metadata = gps_dict[csv_key]
        for row in measurements:
            ts = row.get(DATETIME_KEY) or 'NaT'

            def add_item(var_dict):
                item_dict = {
                    "date_time": ts,
                    "borehole": csv_key,
                    "depth": 0.0,
                    "rock_temp": math.nan,
                    "air_temp": math.nan,
                    "air_humidity": math.nan,
                }
                item_dict.update(borehole_metadata)
                item_dict.update(var_dict)
                records.append(item_dict)

            # --- air temperature item ---
            # --- air humidity item ---
            add_item({
                "air_temp": as_float_or_nan(row.get(AIR_TEMP_KEY, math.nan)),
                "air_humidity": as_float_or_nan(row.get(AIR_HUM_KEY, math.nan))
            })

            # --- rock temperature items (depths only) ---
            for k, v in row.items():
                if DEPTH_KEY_PATTERN.match(k):
                    add_item({
                        "rock_temp": as_float_or_nan(v),
                        "depth": depth_label(k),
                    })

    return records

def read_new_fiedler_json(json_dict) -> dict:

    site_records_dict = {}
    #site_ids = []
    data = json_dict

    # profile_number = sensors_profile.row(by_predicate=((pl.col("sen_lon") == lon) & (pl.col("sen_lat") == lat)))[2]
    # profile_name = str('stanoviste' + profile_number)

    #############################################################################################
    # rearrangement of data according to metadata
    # result = dictionary of sites with measured data
    # "site_name": [{"date": ..., "0.05m": ..., "0.5m": ..., "RVT13-T-Vzduch": ..., ...},{...}, ...]

    site = data["data"][0]["metadata"]["label"]  # site name
    measurements = data["data"][0]["data"]  # list of sensors

    # Take timestamps from the first sensor
    times = [point["t"] for point in measurements[0]["data"]]

    site_records = []
    for t_idx, timestamp in enumerate(times):
        record = {"date": datetime.fromtimestamp(timestamp/1000, tz=timezone.utc).isoformat()}

        for sensor in measurements:
            label = sensor["metadata"]["label"]
            value = sensor["data"][t_idx]["v"]
            record[label] = value

        site_records.append(record)

    site_records_dict[site] = site_records
    #site_ids.append(site)
    #############################################################################################

    return site_records_dict #, site_ids

def read_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data

_SCHEMAS_DIR = Path(__file__).parent.parent / "schemas"


def _load_gps_dict(schemas_dir: Path) -> dict:
    df = pl.read_csv(schemas_dir / "bukov_locations.csv")
    return {
        row["borehole"]: {
            "latitude": float(row["GPS_N"]),
            "longitude": float(row["GPS_E"]),
            "borehole_project_id": row["project_id"],
        }
        for row in df.iter_rows(named=True)
    }


def normalize_new(payload: bytes, metadata: dict) -> pl.DataFrame:
    try:
        json_dict = json.loads(payload.decode("utf-8"))
    except Exception:
        raise ValueError("Failed to parse JSON payload")

    json_dict_new = read_new_fiedler_json(json_dict)
    gps_dict = _load_gps_dict(_SCHEMAS_DIR)
    return pl.DataFrame(normalize(json_dict_new, gps_dict))

def normalize_old(json_dict : dict, gps_dict: dict, metadata : dict) -> dict:
    return normalize(json_dict, gps_dict)
