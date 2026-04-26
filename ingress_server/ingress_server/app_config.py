import os
import yaml
import logging

from pathlib import Path
from threading import Event
from dataclasses import dataclass, field
from typing import Any

from dotenv import load_dotenv

load_dotenv()

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class BaseConfig:
    queue_dir_path: str = "./var/zarr_fuse"
    log_level: str = "INFO"
    port: int = 8000
    worker_poll_interval: int = 30


@dataclass(frozen=True)
class SmtpConfig:
    notify_to: list[str] = field(default_factory=list)
    host: str = ""
    port: int = 587
    from_email: str = ""
    username: str = ""
    password: str = ""

    @property
    def enabled(self) -> bool:
        return bool(self.notify_to and self.host)


@dataclass(frozen=True)
class AppConfig:
    queue_dir: Path
    config_path: Path
    config: dict[str, Any]
    base: BaseConfig
    smtp: SmtpConfig
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


def _parse_base_config(raw: dict) -> BaseConfig:
    return BaseConfig(
        queue_dir_path=raw.get("queue_dir_path", BaseConfig.queue_dir_path),
        log_level=raw.get("log_level", BaseConfig.log_level),
        port=int(raw.get("port", BaseConfig.port)),
        worker_poll_interval=int(raw.get("worker_poll_interval", BaseConfig.worker_poll_interval)),
    )


def _parse_smtp_config(raw: dict) -> SmtpConfig:
    notify_to = raw.get("notify_to", [])
    if isinstance(notify_to, str):
        notify_to = [x.strip() for x in notify_to.split(",") if x.strip()]

    password = (os.getenv("SMTP_PASSWORD") or raw.get("password", "")).strip()

    return SmtpConfig(
        notify_to=list(notify_to),
        host=raw.get("host", ""),
        port=int(raw.get("port", SmtpConfig.port)),
        from_email=raw.get("from_email", ""),
        username=raw.get("username", ""),
        password=password,
    )


def load_app_config(config_path: str | Path) -> "AppConfig":
    config_path = Path(config_path).resolve()

    try:
        with config_path.open("r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
    except Exception:
        LOG.exception("Failed to load configuration file %s", config_path)
        raise

    cfg_block = config.get("configuration", {})
    base = _parse_base_config(cfg_block.get("base", {}))
    smtp = _parse_smtp_config(cfg_block.get("smtp", {}))

    queue_dir = Path(base.queue_dir_path).resolve()

    app_config = AppConfig(
        queue_dir=queue_dir,
        config_path=config_path,
        config=config,
        base=base,
        smtp=smtp,
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
