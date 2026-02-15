import os
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import Optional, Dict, Any
import yaml
from dotenv import load_dotenv

load_dotenv()

STOP = Event()

@dataclass(frozen=True)
class Settings:
    queue_dir: Path
    config_dir: Path

    @property
    def accepted_dir(self) -> Path: return self.queue_dir / "accepted"

    @property
    def success_dir(self) -> Path: return self.queue_dir / "success"

    @property
    def failed_dir(self) -> Path: return self.queue_dir / "failed"


_SETTINGS: Optional[Settings] = None
CONFIG: Dict[str, Any] = {}


def init_settings(queue_dir: Path = None, config_dir: Path = None) -> Settings:
    global _SETTINGS, CONFIG

    queue_dir_path = Path(
        queue_dir if queue_dir is not None
        else os.getenv("QUEUE_DIR_PATH", "./var/zarr_fuse")
    ).resolve()
    config_dir_path = Path(
        config_dir if config_dir is not None
        else os.getenv("CONFIG_DIR_PATH", "inputs")
    ).resolve()

    (queue_dir_path / "accepted").mkdir(parents=True, exist_ok=True)
    (queue_dir_path / "success").mkdir(parents=True, exist_ok=True)
    (queue_dir_path / "failed").mkdir(parents=True, exist_ok=True)

    with (config_dir_path / "endpoints_config.yaml").open("r", encoding="utf-8") as f:
        CONFIG = yaml.safe_load(f) or {}

    _SETTINGS = Settings(queue_dir=queue_dir_path, config_dir=config_dir_path)
    return _SETTINGS

def get_settings() -> Settings:
    global _SETTINGS
    if _SETTINGS is None:
        _SETTINGS = init_settings()
    return _SETTINGS

def config_dir() -> Path:
    return get_settings().config_dir

def queue_dir() -> Path:
    return get_settings().queue_dir
