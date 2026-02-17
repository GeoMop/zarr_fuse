import os
import yaml
from threading import Event
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

STOP = Event()
QUEUE_BASE_DIR = Path(os.getenv("QUEUE_DIR", "./var/zarr_fuse"))
ACCEPTED_DIR = QUEUE_BASE_DIR / "accepted"
SUCCESS_DIR = QUEUE_BASE_DIR / "success"
FAILED_DIR = QUEUE_BASE_DIR / "failed"

for d in (ACCEPTED_DIR, SUCCESS_DIR, FAILED_DIR):
    d.mkdir(parents=True, exist_ok=True)

with open(os.getenv("CONFIG_PATH", "inputs") + "/endpoints_config.yaml", "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f) or {}
