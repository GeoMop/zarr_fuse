import os
import yaml
from threading import Event
from pathlib import Path


STOP = Event()
BASE_DIR = Path(os.getenv("QUEUE_DIR", "./var/zarr_fuse"))
ACCEPTED_DIR = BASE_DIR / "accepted"
SUCCESS_DIR = BASE_DIR / "success"
FAILED_DIR = BASE_DIR / "failed"
for d in (ACCEPTED_DIR, SUCCESS_DIR, FAILED_DIR):
    d.mkdir(parents=True, exist_ok=True)

def _resolve_inputs_prefix() -> Path:
    path_prefix = Path(__file__).parent
    if os.getenv("PRODUCTION", "false").lower() == "true":
        path_prefix /= "inputs/prod"
    else:
        path_prefix /= "inputs/ci"
    return path_prefix

PATH_PREFIX = _resolve_inputs_prefix()
CONFIG_PATH = PATH_PREFIX / "endpoints_config.yaml"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f) or {}

ENDPOINT_NAME_TO_SCHEMA: dict[str, Path] = {}
for ep in CONFIG.get("endpoints", []):
    name = ep["name"]
    schema_path = PATH_PREFIX / ep["schema_path"]
    ENDPOINT_NAME_TO_SCHEMA[name] = schema_path
