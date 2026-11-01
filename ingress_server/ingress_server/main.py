import os
import logging
from contextlib import asynccontextmanager
from threading import Thread

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler

from .models import EndpointConfig
from .logging_setup import setup_logging
from .app_config import load_app_config, AppConfig
from .worker import startup_recover, install_signal_handlers, working_loop
from .active_scrapper import register_active_scrapper, ActiveScrapperConfig
from .passive_scrapper import register_passive_scrapper


# =========================
# Configuration
# =========================
load_dotenv()
setup_logging()

BG_SCHEDULER = BackgroundScheduler()
LOG = logging.getLogger(__name__)


# =========================
# Runtime configuration
# =========================
def load_runtime_config() -> AppConfig:
    config_path = os.getenv("CONFIG_PATH", "inputs/endpoints_config.yaml")

    LOG.info("Loading runtime configuration config_path=%s", config_path)

    app_config = load_app_config(config_path=config_path)

    setup_logging(app_config.base.log_level)
    LOG.info("Log level set to %s", app_config.base.log_level)

    return app_config


# =========================
# Worker management
# =========================
def _start_worker_thread(app: FastAPI, app_config: AppConfig) -> Thread:
    LOG.info("Starting worker thread")
    thread = Thread(
        target=working_loop,
        args=(app_config, float(app_config.base.worker_poll_interval)),
        name="worker",
        daemon=True,
    )
    thread.start()

    app.state.worker_thread = thread
    LOG.info("Worker thread started name=%s alive=%s", thread.name, thread.is_alive())
    return thread


def _graceful_shutdown(app: FastAPI) -> None:
    try:
        if BG_SCHEDULER.running:
            BG_SCHEDULER.shutdown(wait=False)
            LOG.info("Background scheduler stopped")
    except Exception:
        LOG.exception("Failed to shutdown background scheduler")

    app_config: AppConfig | None = getattr(app.state, "app_config", None)
    if app_config:
        app_config.stop_event.set()

    thread = getattr(app.state, "worker_thread", None)
    if thread:
        thread.join(timeout=10)
        LOG.info("Worker thread stopped name=%s alive=%s", thread.name, thread.is_alive())


def bootstrap_runtime(app: FastAPI, app_config: AppConfig) -> None:
    LOG.info("Bootstrapping runtime")
    startup_recover(app_config)
    LOG.info("Startup recovery finished")
    install_signal_handlers(app_config)
    LOG.debug("Signal handlers installed")
    _start_worker_thread(app, app_config)
    LOG.info("Runtime bootstrap finished")


# =========================
# API endpoints
# =========================
def register_health_endpoint(app: FastAPI) -> None:
    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}


def register_app_components(app: FastAPI, app_config: AppConfig) -> None:
    config = app_config.config
    LOG.debug("Configuration: %s", config)

    register_health_endpoint(app)

    for ep in config.get("endpoints", []):
        model = EndpointConfig.model_validate(ep)
        register_passive_scrapper(app, app_config, model)

    for scrapper in config.get("active_scrappers", []):
        model = ActiveScrapperConfig.model_validate(scrapper)
        register_active_scrapper(app_config, model, BG_SCHEDULER)

    if not BG_SCHEDULER.running:
        BG_SCHEDULER.start()
        LOG.info("Background scheduler started")
    else:
        LOG.debug("Background scheduler already running")


# =========================
# FastAPI lifecycle
# =========================
@asynccontextmanager
async def lifespan(app: FastAPI):
    app_config = load_runtime_config()
    app.state.app_config = app_config

    register_app_components(app, app_config)
    bootstrap_runtime(app, app_config)

    try:
        yield
    finally:
        LOG.info("Application shutdown started")
        _graceful_shutdown(app)
        LOG.info("Application shutdown finished")


# =========================
# App factory
# =========================
def create_app() -> FastAPI:
    return FastAPI(lifespan=lifespan)


# =========================
# Entrypoint
# =========================
def main() -> None:
    config_path = os.getenv("CONFIG_PATH", "inputs/endpoints_config.yaml")
    app_config = load_app_config(config_path=config_path)
    port = app_config.base.port

    LOG.info("Starting ingress server host=0.0.0.0 port=%s", port)
    uvicorn.run(
        create_app(),
        host="0.0.0.0",
        port=port,
        reload=False,
        log_config=None,
    )


if __name__ == "__main__":
    main()
