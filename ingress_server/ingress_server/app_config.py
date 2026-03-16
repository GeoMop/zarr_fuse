import yaml
import logging

from pathlib import Path
from threading import Event
from dataclasses import dataclass, field
from typing import Any

LOG = logging.getLogger(__name__)

@dataclass(frozen=True)
class AppConfig:
    queue_dir: Path
    config_path: Path
    config: dict[str, Any]
    stop_event: Event = field(default_factory=Event, compare=False)

    @property
    def config_dir(self) -> Path:
        return self.config_path.parent

    @property
    def accepted_dir(self) -> Path:
        return self.queue_dir / "accepted"

    @property
    def success_dir(self) -> Path:
        return self.queue_dir / "success"

    @property
    def failed_dir(self) -> Path:
        return self.queue_dir / "failed"


def load_app_config(config_path: str | Path, queue_dir: str | Path) -> AppConfig:
    config_path = Path(config_path).resolve()
    queue_dir = Path(queue_dir).resolve()

    try:
        with config_path.open("r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
    except Exception:
        LOG.exception("Failed to load configuration file %s", config_path)
        raise

    app_config = AppConfig(
        queue_dir=queue_dir,
        config_path=config_path,
        config=config,
    )

    app_config.accepted_dir.mkdir(parents=True, exist_ok=True)
    app_config.success_dir.mkdir(parents=True, exist_ok=True)
    app_config.failed_dir.mkdir(parents=True, exist_ok=True)

    LOG.info(
        "Application config loaded. queue_dir=%s accepted=%s success=%s failed=%s",
        queue_dir,
        app_config.accepted_dir,
        app_config.success_dir,
        app_config.failed_dir,
    )

    return app_config
