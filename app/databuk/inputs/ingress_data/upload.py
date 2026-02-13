import zarr_fuse as zf
import numpy as np

from importlib import import_module

def resolve_extractor(fn_module: str, extract_fn: str):
    if not fn_module or not extract_fn:
        return None
    try:
        module = import_module(fn_module)
        return getattr(module, extract_fn)
    except Exception as e:
        raise ModuleNotFoundError(f"Failed to resolve extractor {extract_fn} from {fn_module}")

import sys
from pathlib import Path
sys.path.append("..")
import extract as ex
import pandas as pd

for f in sorted(Path('.').glob('*.json')):
    metadata = {}
    df_list = ex.normalize_old(ex.read_json(f), metadata)
    df = pd.DataFrame(df_list)
    print(f)
    print(df.info())
    print(df.head())
    print(np.unique([str(type(dt)) for dt in df['date_time']]))

    vals = df['date_time'].to_list()
    # find position of first None
    try:
        idx = next(i for i, v in enumerate(vals) if v == 'NaT')
    except StopIteration:
        print("No None found in df['date_time']")
    else:
        start = max(0, idx - 10)
        end = min(len(vals), idx + 10)  # +3 because slice end is exclusive

        print(f"First None at position {idx}, showing [{start}:{end}]:")
        for i in range(start, end):
            print(i, vals[i], type(vals[i]))

    root_node = zf.open_store('../schemas/bukov_schema.yaml')
    bukov_node = root_node['bukov']
    bukov_node.update(df)
    print("  'date_time' extent:", bukov_node.dataset['date_time'].min(), bukov_node.dataset['date_time'].max())