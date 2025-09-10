import os
import yaml
from threading import Event
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

STOP = Event()
BASE_DIR = Path(os.getenv("QUEUE_DIR", "./var/zarr_fuse"))
ACCEPTED_DIR = BASE_DIR / "accepted"
SUCCESS_DIR = BASE_DIR / "success"
FAILED_DIR = BASE_DIR / "failed"

for d in (ACCEPTED_DIR, SUCCESS_DIR, FAILED_DIR):
    d.mkdir(parents=True, exist_ok=True)

with open("inputs/endpoints_config.yaml", "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f) or {}

ENDPOINT_NAME_TO_SCHEMA: dict[str, Path] = {}
for ep in CONFIG.get("endpoints", []):
    name = ep["name"]
    schema_path = "inputs" / ep["schema_path"]
    ENDPOINT_NAME_TO_SCHEMA[name] = schema_path
