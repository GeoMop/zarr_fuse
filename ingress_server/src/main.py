import os
import logging
import atexit

from threading import Thread
from flask import Flask, request, jsonify
from dotenv import load_dotenv

from auth import AUTH
from io_utils import process_payload
from configs import CONFIG, STOP
from worker import startup_recover, install_signal_handlers, working_loop
from logging_setup import setup_logging
from active_scrapper import add_scrapper_job

from models import ActiveScrapperConfig, EndpointConfig
from apscheduler.schedulers.background import BackgroundScheduler

BG_SCHEDULER = BackgroundScheduler()

load_dotenv()
APP = Flask(__name__)
handler = setup_logging()
LOG = logging.getLogger("ingress")

# =========================
# Flask handlers
# =========================
@APP.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

@AUTH.login_required
def _upload_node(endpoint_config: EndpointConfig, node_path: str = ""):
    LOG.debug("ingress.request endpoint=%s node_path=%r ct=%r user=%r",
        endpoint_config.name, node_path, request.headers.get("Content-Type"), AUTH.current_user())

    content_type = (request.headers.get("Content-Type") or "").lower()
    data = request.get_data()

    try:
        success, err = process_payload(
            data_source=endpoint_config.data_source,
            payload=data,
            content_type=content_type,
            username=AUTH.current_user(),
            node_path=node_path,
        )
        if not success:
            LOG.warning("Processing failed for endpoint=%s node_path=%s: %s", endpoint_config.name, node_path, err)
            return jsonify({"error": err}), 400
    except Exception as e:
        LOG.warning("Saving data failed for endpoint=%s node_path=%s: %s", endpoint_config.name, node_path, e)
        return jsonify({"error": str(e)}), 400

    LOG.info(f"""Successfully accepted data for endpoint: {endpoint_config.name},
            node_path: {node_path}, user: {AUTH.current_user()},
            content_type: {content_type}, bytes: {len(data)}""")
    return jsonify({"status": "accepted"}), 202


# =========================
# Route creation
# =========================
def create_upload_endpoint(config: EndpointConfig):
    # Root path (without node_path)
    APP.add_url_rule(
        config.endpoint,
        endpoint=f"upload_node_root_{config.name.replace('-', '_')}",
        view_func=_upload_node,
        methods=["POST"],
        defaults={"endpoint_config": config, "node_path": ""},
    )

    # Subpath with node_path
    APP.add_url_rule(
        f"{config.endpoint}/<path:node_path>",
        endpoint=f"upload_node_sub_{config.name.replace('-', '_')}",
        view_func=_upload_node,
        methods=["POST"],
        defaults={"endpoint_config": config},
    )


# =========================
# App creation and worker thread
# =========================
def create_app():
    for ep in CONFIG.get("endpoints", []):
        model = EndpointConfig.model_validate(ep)
        create_upload_endpoint(model)
        LOG.info("Created upload endpoint %s at %s", model.name, model.endpoint)

    for scrapper in CONFIG.get("active_scrappers", []):
        model = ActiveScrapperConfig.model_validate(scrapper)
        add_scrapper_job(model, BG_SCHEDULER)
        LOG.info("Created active scrapper job %s for %s", model.name, model.url)
    BG_SCHEDULER.start()
    return APP

def _start_worker_thread():
    t = Thread(target=working_loop, name="worker", daemon=True)
    t.start()
    APP.config["worker_thread"] = t
    return t

def _graceful_shutdown():
    BG_SCHEDULER.shutdown(wait=False)
    STOP.set()
    t = APP.config.get("worker_thread")
    if t:
        t.join(timeout=10)

def create_app_with_worker():
    startup_recover()
    install_signal_handlers()
    _start_worker_thread()

    atexit.register(_graceful_shutdown)

    return create_app()


# =========================
# Main
# =========================
if __name__ == "__main__":
    create_app()
    startup_recover()
    install_signal_handlers()

    t = _start_worker_thread()
    try:
        APP.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)), debug=False, use_reloader=False)
    except KeyboardInterrupt:
        LOG.info("KeyboardInterrupt - shutting down…")
    finally:
        LOG.info("Waiting for worker to stop…")
        _graceful_shutdown()
