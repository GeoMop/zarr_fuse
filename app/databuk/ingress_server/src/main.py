import os
import json
import logging
import time

from threading import Thread
from flask import Flask, request, jsonify, g
from dotenv import load_dotenv

from .auth import AUTH, AUTH_ENABLED, auth_wrapper
from .io_utils import validate_content_type, sanitize_node_path, atomic_write, new_msg_path, validate_data
from .configs import CONFIG, ACCEPTED_DIR, STOP
from .worker import startup_recover, install_signal_handlers, working_loop
from .logging_setup import setup_logging


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


def _upload_node(endpoint_name: str, node_path: str = ""):
    username = AUTH.current_user() if AUTH_ENABLED else "anonymous"

    LOG.debug("ingress.request endpoint=%s node_path=%r ct=%r user=%r",
        endpoint_name, node_path, request.headers.get("Content-Type"), username)

    content_type = (request.headers.get("Content-Type") or "").lower()
    ok, err = validate_content_type(content_type)
    if not ok:
        LOG.warning("Invalid content type: %s", content_type)
        return jsonify({"error": err}), 415 if "Unsupported" in err else 400

    data = request.get_data()
    ok, err = validate_data(data, content_type)
    if not ok:
        LOG.warning("Invalid data: %s", err)
        return jsonify({"error": err}), 400

    try:
        safe_child = sanitize_node_path(node_path)
    except ValueError as e:
        LOG.warning("Invalid node_path: %s", node_path)
        return jsonify({"error": str(e)}), 400

    base = (ACCEPTED_DIR / endpoint_name) / safe_child
    suffix = ".csv" if "csv" in content_type else ".json"
    msg_path = new_msg_path(base, suffix)

    atomic_write(msg_path, data)

    meta_data = {
        "content_type": content_type,
        "node_path": node_path,
        "endpoint_name": endpoint_name,
        "username": username,
        "received_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    atomic_write(msg_path.with_suffix(msg_path.suffix + ".meta.json"), json.dumps(meta_data).encode("utf-8"))

    LOG.info("ingress.accepted endpoint=%s node_path=%s path=%s user=%s ct=%s bytes=%d",
            endpoint_name, node_path, msg_path, meta_data["username"],
            content_type, len(data))
    return jsonify({"status": "accepted"}), 202


# =========================
# Route creation
# =========================
def create_upload_endpoint(endpoint_name: str, endpoint_url: str):
    wrapped = auth_wrapper(_upload_node)

    # Root path (without node_path)
    APP.add_url_rule(
        endpoint_url,
        endpoint=f"upload_node_root_{endpoint_name.replace('-', '_')}",
        view_func=wrapped,
        methods=["POST"],
        defaults={"endpoint_name": endpoint_name, "node_path": ""},
    )

    # Subpath with node_path
    APP.add_url_rule(
        f"{endpoint_url}/<path:node_path>",
        endpoint=f"upload_node_sub_{endpoint_name.replace('-', '_')}",
        view_func=wrapped,
        methods=["POST"],
        defaults={"endpoint_name": endpoint_name},
    )


# =========================
# Main
# =========================
if __name__ == "__main__":
    LOG.info("Starting ingress server…")
    for ep in CONFIG.get("endpoints", []):
        create_upload_endpoint(ep["name"], ep["endpoint"])

    startup_recover()
    install_signal_handlers()

    worker = Thread(target=working_loop, name="worker", daemon=True)
    worker.start()

    try:
        APP.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)), debug=False, use_reloader=False)
    except KeyboardInterrupt:
        LOG.info("KeyboardInterrupt - shutting down…")
    finally:
        LOG.info("Waiting for worker to stop…")
        STOP.set()
        worker.join(timeout=10)
