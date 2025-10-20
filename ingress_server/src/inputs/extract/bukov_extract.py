import json
import re
import math
#import csv
import polars as pl
from datetime import datetime
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
        return None

def depth_label(key: str) -> str:
    if key == "15mA": # convert '15mA' -> '15.1'
        return "15.1"
    if key == "15mB": # convert '15mB' -> '15.1'
        return "15.2"
    return key[:-1] if key.endswith("m") else key  # '0.05m' -> '0.05' etc.

def normalize(json_dict):
    data = json_dict
    records = []

    for borehole, measurements in data.items():
        for row in measurements:
            ts = row.get(DATETIME_KEY)

            # --- air temperature item ---
            records.append({
                "date_time": ts,
                "borehole":  borehole,
                # "depth": None,
                "depth": "0",
                "rock_temp": None,
                "air_temp":  as_float_or_nan(row.get(AIR_TEMP_KEY, None)),
                "air_humidity": None,

            })

            # --- air humidity item ---
            records.append({
                "date_time": ts,
                "borehole":  borehole,
                # "depth": None,
                "depth": "0.01",
                "rock_temp": None,
                "air_temp":  None,
                "air_humidity": as_float_or_nan(row.get(AIR_HUM_KEY, None)),
                #"depth": None,
            })

            # --- rock temperature items (depths only) ---
            for k, v in row.items():
                if DEPTH_KEY_PATTERN.match(k):
                    records.append({
                        "date_time": ts,
                        "borehole":  borehole,
                        "rock_temp": as_float_or_nan(v),
                        "air_temp":  None,
                        "air_humidity": None,
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
        record = {"date": datetime.fromtimestamp(timestamp/1000).isoformat() + '+00:00'}

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

def normalize_new(json_dict : dict) -> pl.DataFrame:
    json_dict_new = read_new_fiedler_json(json_dict)
    return pl.DataFrame(normalize(json_dict_new))

def normalize_old(json_dict : dict) -> pl.DataFrame:
    return pl.DataFrame(normalize(json_dict))
