import os
import yaml
import logging

from pathlib import Path
from threading import Event
from dataclasses import dataclass, field
from typing import Any

from dotenv import load_dotenv

from .local_cache import LocalCache
from .manifest import ManifestStore, default_manifest_url
from .queue_storage import QueueStorage

load_dotenv()

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class BaseConfig:
    queue_dir_path: str = "./var/zarr_fuse"
    log_level: str = "INFO"
    port: int = 8000
    worker_poll_interval: int = 30
    # Retention window (in hours) for the time filter (see io/time_filter.py):
    # an item is held until it is more than this much older than the newest
    # time_like_coord value seen in the same batch. 0 disables holding.
    retention_time: float = 96.0
    manifest_url: str | None = None
    cache_dir: str | None = None


@dataclass(frozen=True)
class SmtpConfig:
    notify_to: list[str] = field(default_factory=list)
    host: str = ""
    port: int = 587
    from_email: str = ""
    username: str = ""
    password: str = ""


@dataclass(frozen=True)
class AppConfig:
    queue: QueueStorage
    manifest: ManifestStore
    config_path: Path
    config: dict[str, Any]
    base: BaseConfig
    smtp: SmtpConfig
    cache: LocalCache = field(default_factory=lambda: LocalCache(None))
    stop_event: Event = field(default_factory=Event, compare=False)
    # Anomalies already emailed, so a batch that keeps being re-examined on
    # every worker poll does not re-send the same notification.
    notified_anomalies: set[str] = field(default_factory=set, compare=False)

    @property
    def config_dir(self) -> Path:
        return self.config_path.parent


def _parse_base_config(raw: dict) -> BaseConfig:
    return BaseConfig(
        queue_dir_path=os.getenv("QUEUE_DIR_PATH", raw.get("queue_dir_path", BaseConfig.queue_dir_path)),
        log_level=raw.get("log_level", BaseConfig.log_level),
        port=int(os.getenv("PORT", raw.get("port", BaseConfig.port))),
        worker_poll_interval=int(raw.get("worker_poll_interval", BaseConfig.worker_poll_interval)),
        retention_time=float(raw.get("retention_time", BaseConfig.retention_time)),
        manifest_url=raw.get("manifest_url", BaseConfig.manifest_url),
        cache_dir=raw.get("cache_dir", BaseConfig.cache_dir),
    )


def _parse_smtp_config(raw: dict) -> SmtpConfig:
    notify_to = raw.get("notify_to", [])
    if isinstance(notify_to, str):
        notify_to = [x.strip() for x in notify_to.split(",") if x.strip()]

    smtp_password_env = os.getenv("SMTP_PASSWORD")
    password = smtp_password_env.strip() if smtp_password_env else ""

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

    queue = QueueStorage(base.queue_dir_path)
    manifest = ManifestStore(base.manifest_url or default_manifest_url(queue.url))
    cache = LocalCache(base.cache_dir)

    app_config = AppConfig(
        queue=queue,
        manifest=manifest,
        config_path=config_path,
        config=config,
        base=base,
        smtp=smtp,
        cache=cache,
    )

    queue.ensure_layout()
    manifest.ensure_store()

    LOG.info(
        "Application config loaded. queue=%s manifest=%s cache=%s",
        queue.url,
        manifest.url,
        cache.root,
    )

    return app_config
