import os
import logging
import atexit

from threading import Thread
from flask import Flask, request, jsonify
from dotenv import load_dotenv

from .auth import AUTH
from .models import EndpointConfig
from .io import process_payload
from .app_config import load_app_config, AppConfig
from .worker import startup_recover, install_signal_handlers, working_loop
from .logging_setup import setup_logging
from .active_scrapper.scheduler import add_scrapper_jobs
from .active_scrapper.active_scrapper_config_models import ActiveScrapperConfig

from apscheduler.schedulers.background import BackgroundScheduler
BG_SCHEDULER = BackgroundScheduler()


load_dotenv()
setup_logging()
APP = Flask(__name__)
LOG = logging.getLogger(__name__)


# =========================
# Flask handlers
# =========================
@APP.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@AUTH.login_required
def _upload_node(app_config, endpoint_config: EndpointConfig, node_path: str = ""):
    user = AUTH.current_user()
    content_type = (request.headers.get("Content-Type") or "").lower()
    data = request.get_data()

    LOG.debug(
        "Received upload request endpoint=%s node_path=%r content_type=%r user=%r bytes=%s",
        endpoint_config.name,
        node_path,
        content_type,
        user,
        len(data),
    )

    content_type = (request.headers.get("Content-Type") or "").lower()
    data = request.get_data()

    try:
        process_payload(
            app_config=app_config,
            data_source=endpoint_config.data_source,
            payload=data,
            content_type=content_type,
            username=user,
            node_path=node_path,
        )
    except ValueError as exc:
        LOG.warning(
            "Rejected payload for endpoint=%s node_path=%r user=%r: %s",
            endpoint_config.name,
            node_path,
            user,
            exc,
        )
        return jsonify({"error": str(exc)}), 400
    except Exception:
        LOG.exception(
            "Unexpected processing failure for endpoint=%s node_path=%r user=%r",
            endpoint_config.name,
            node_path,
            user,
        )
        return jsonify({"error": "Internal processing error"}), 500

    LOG.info(
        "Accepted upload for endpoint=%s node_path=%r user=%r content_type=%r bytes=%s",
        endpoint_config.name,
        node_path,
        user,
        content_type,
        len(data),
    )
    return jsonify({"status": "accepted"}), 202


# =========================
# Route creation
# =========================
def create_upload_endpoint(app_config: AppConfig, config: EndpointConfig):
    # Root path (without node_path)
    APP.add_url_rule(
        config.endpoint,
        endpoint=f"upload_node_root_{config.name.replace('-', '_')}",
        view_func=_upload_node,
        methods=["POST"],
        defaults={
            "app_config": app_config,
            "endpoint_config": config,
            "node_path": ""
        },
    )

    # Subpath with node_path
    APP.add_url_rule(
        f"{config.endpoint}/<path:node_path>",
        endpoint=f"upload_node_sub_{config.name.replace('-', '_')}",
        view_func=_upload_node,
        methods=["POST"],
        defaults={
            "app_config": app_config,
            "endpoint_config": config,
        },
    )



# =========================
# Application factory
# =========================
def create_app(app_config: AppConfig):
    config = app_config.config
    LOG.debug("Configuration: %s", config)

    for ep in config.get("endpoints", []):
        model = EndpointConfig.model_validate(ep)
        create_upload_endpoint(app_config, model)
        LOG.info("Created upload endpoint name=%s path=%s", model.name, model.endpoint)

    for scrapper in config.get("active_scrappers", []):
        model = ActiveScrapperConfig.model_validate(scrapper)
        add_scrapper_jobs(app_config, model, BG_SCHEDULER)
        LOG.info("Created active scrapper jobs for name=%s url=%s", model.name, model.request.url)

    if not BG_SCHEDULER.running:
        BG_SCHEDULER.start()
        LOG.info("Background scheduler started")
    else:
        LOG.debug("Background scheduler already running")

    APP.config["app_config"] = app_config
    return APP


# =========================
# Runtime bootstrap
# =========================
def load_runtime_config() -> AppConfig:
    config_path = os.getenv("CONFIG_PATH", "inputs/endpoints_config.yaml")
    queue_dir = os.getenv("QUEUE_DIR_PATH", "./var/zarr_fuse")

    LOG.info(
        "Loading runtime configuration config_path=%s queue_dir=%s",
        config_path,
        queue_dir,
    )

    return load_app_config(
        config_path=config_path,
        queue_dir=queue_dir,
    )


def _start_worker_thread(app_config: AppConfig):
    LOG.info("Starting worker thread")
    thread = Thread(
        target=working_loop,
        args=(app_config, float(os.getenv("WORKER_POLL_INTERVAL", 30))),
        name="worker",
        daemon=True,
    )
    thread.start()

    APP.config["worker_thread"] = thread
    LOG.info("Worker thread started name=%s alive=%s", thread.name, thread.is_alive())
    return thread


def _graceful_shutdown():
    try:
        BG_SCHEDULER.shutdown(wait=False)
    except Exception:
        pass

    app_config: AppConfig | None = APP.config.get("app_config")
    if app_config:
        app_config.stop_event.set()

    t = APP.config.get("worker_thread")
    if t:
        t.join(timeout=10)


def bootstrap_runtime(app_config: AppConfig):
    LOG.info("Bootstrapping runtime")

    startup_recover(app_config)
    LOG.info("Startup recovery finished")

    install_signal_handlers(app_config)
    LOG.debug("Signal handlers installed")

    _start_worker_thread(app_config)
    atexit.register(_graceful_shutdown)
    LOG.info("Runtime bootstrap finished")


def create_app_with_worker():
    app_config = load_runtime_config()
    app = create_app(app_config)
    bootstrap_runtime(app_config)
    return app


def main():
    app = create_app_with_worker()
    port = int(os.getenv("PORT", 8000))

    LOG.info("Starting ingress server host=0.0.0.0 port=%s", port)
    try:
        app.run(
            host="0.0.0.0",
            port=port,
            debug=False,
            use_reloader=False,
        )
    except KeyboardInterrupt:
        LOG.info("KeyboardInterrupt - shutting down…")
    except Exception:
        LOG.exception("Ingress server crashed")
        raise
    finally:
        LOG.info("Waiting for worker to stop…")
        _graceful_shutdown()


# =========================
# Entrypoint
# =========================
if __name__ == "__main__":
    main()
