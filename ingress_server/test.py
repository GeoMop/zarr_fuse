import json
import bz2
import tempfile
from pathlib import Path
import xarray as xr

from pathlib import Path
from ingress_server.inputs.extract.chmi_aladin_1km_extract import my_extractor


data_path = Path(__file__).parent / "var" / "zarr_fuse" / "failed" / "chmi-aladin-1km" / "20260222T200600_cd3d16ee6c62.grib.bz2"
#payload_path = test_data_dir / "20260222T200600_cd3d16ee6c62.grib.bz2"
#metadata_path = test_data_dir / "20260222T200600_cd3d16ee6c62.grib.bz2.meta.json"
#payload = Path(payload_path).read_bytes()
#metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
#result = my_extractor(payload, metadata)

payload = Path(data_path).read_bytes()

data = bz2.decompress(payload)

with tempfile.TemporaryDirectory() as td:
    p = Path(td) / "input.grib"
    p.write_bytes(data)

    ds = xr.open_dataset(
        p,
        engine="cfgrib",
        backend_kwargs={"indexpath": ""},
    ).load()

print("\n=== DATA VARS ===")
print(list(ds.data_vars))

print("\n=== COORDS ===")
print(list(ds.coords))

# === DATA VARS === ['msl']
# === COORDS === ['time', 'step', 'meanSea', 'latitude', 'longitude', 'valid_time']"
