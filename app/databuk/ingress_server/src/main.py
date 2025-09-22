import os
import json
import logging
import time
import atexit

from threading import Thread
from flask import Flask, request, jsonify
from dotenv import load_dotenv

from auth import AUTH
from io_utils import validate_content_type, sanitize_node_path, atomic_write, new_msg_path, validate_data
from configs import CONFIG, ACCEPTED_DIR, STOP
from worker import startup_recover, install_signal_handlers, working_loop
from logging_setup import setup_logging

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
def _upload_node(endpoint_name: str, node_path: str = ""):
    LOG.debug("ingress.request endpoint=%s node_path=%r ct=%r user=%r",
        endpoint_name, node_path, request.headers.get("Content-Type"), AUTH.current_user())

    content_type = (request.headers.get("Content-Type") or "").lower()
    ok, err = validate_content_type(content_type)
    if not ok:
        LOG.warning("Validation content type failed for %s: %s", content_type, err)
        return jsonify({"error": err}), 415 if "Unsupported" in err else 400

    data = request.get_data()
    ok, err = validate_data(data, content_type)
    if not ok:
        LOG.warning("Validating data failed for %s", err)
        return jsonify({"error": err}), 400

    safe_child, err = sanitize_node_path(node_path)
    if err:
        LOG.warning("Sanitizing node_path failed for %s: %s", node_path, err)
        return jsonify({"error": err}), 400

    base = (ACCEPTED_DIR / endpoint_name) / safe_child
    suffix = ".csv" if "csv" in content_type else ".json"
    msg_path = new_msg_path(base, suffix)

    atomic_write(msg_path, data)

    meta_data = {
        "content_type": content_type,
        "node_path": node_path,
        "endpoint_name": endpoint_name,
        "username": AUTH.current_user(),
        "received_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    atomic_write(msg_path.with_suffix(msg_path.suffix + ".meta.json"), json.dumps(meta_data).encode("utf-8"))

    LOG.info("Accepted data for endpoint=%s node_path=%s path=%s user=%s ct=%s bytes=%d",
            endpoint_name, node_path, msg_path, meta_data["username"],
            content_type, len(data))
    return jsonify({"status": "accepted"}), 202


# =========================
# Route creation
# =========================
def create_upload_endpoint(endpoint_name: str, endpoint_url: str):
    # Root path (without node_path)
    APP.add_url_rule(
        endpoint_url,
        endpoint=f"upload_node_root_{endpoint_name.replace('-', '_')}",
        view_func=_upload_node,
        methods=["POST"],
        defaults={"endpoint_name": endpoint_name, "node_path": ""},
    )

    # Subpath with node_path
    APP.add_url_rule(
        f"{endpoint_url}/<path:node_path>",
        endpoint=f"upload_node_sub_{endpoint_name.replace('-', '_')}",
        view_func=_upload_node,
        methods=["POST"],
        defaults={"endpoint_name": endpoint_name},
    )


# =========================
# App creation and worker thread
# =========================
def create_app():
    for ep in CONFIG.get("endpoints", []):
        create_upload_endpoint(ep["name"], ep["endpoint"])
    return APP

def _start_worker_thread():
    t = Thread(target=working_loop, name="worker", daemon=True)
    t.start()
    APP.config["worker_thread"] = t
    return t

def _graceful_shutdown():
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
