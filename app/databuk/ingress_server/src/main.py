import io
import os
import yaml
import json
import logging
import signal
import time, uuid, shutil

import polars as pl
import zarr_fuse as zf

from threading import Event, Thread

from pathlib import Path
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from flask_httpauth import HTTPBasicAuth

# =========================
# Bootstrap & Globals
# =========================
load_dotenv()
APP = Flask(__name__)
LOG = logging.getLogger("ingress")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))


# =========================
# Authentication
# =========================
def _parse_users_json(raw: str | None) -> dict:
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        LOG.warning("[WARN] BASIC_AUTH_USERS_JSON invalid: %s; value=%r", e, raw)
        return {}

AUTH = HTTPBasicAuth()
USERS = _parse_users_json(os.getenv("BASIC_AUTH_USERS_JSON"))
AUTH_ENABLED = os.getenv("AUTH_ENABLED", "true").lower() == "true"

@AUTH.verify_password
def verify_password(username, password):
    if username in USERS and USERS[username] == password:
        return username
    return None


# =========================
# Config & Paths
# =========================
BASE_DIR = Path(os.getenv("QUEUE_DIR", "./var/zarr_fuse"))
ACCEPTED_DIR = BASE_DIR / "accepted"
SUCCESS_DIR = BASE_DIR / "success"
FAILED_DIR = BASE_DIR / "failed"
for d in (ACCEPTED_DIR, SUCCESS_DIR, FAILED_DIR):
    d.mkdir(parents=True, exist_ok=True)

def _resolve_inputs_prefix() -> Path:
    path_prefix = Path(__file__).parent
    if os.getenv("PRODUCTION", "false").lower() == "true":
        path_prefix /= "inputs/prod"
    else:
        path_prefix /= "inputs/ci"
    return path_prefix

PATH_PREFIX = _resolve_inputs_prefix()
CONFIG_PATH = PATH_PREFIX / "endpoints_config.yaml"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f) or {}

ENDPOINT_NAME_TO_SCHEMA: dict[str, Path] = {}
for ep in CONFIG.get("endpoints", []):
    name = ep["name"]
    schema_path = PATH_PREFIX / ep["schema_path"]
    ENDPOINT_NAME_TO_SCHEMA[name] = schema_path


# =========================
# S3 / zarr_fuse helpers
# =========================
def _get_env_vars():
    s3_key = os.getenv("S3_ACCESS_KEY")
    s3_sec = os.getenv("S3_SECRET_KEY")

    if s3_key is None:
        raise ValueError("S3 access key must be provided")
    if s3_sec is None:
        raise ValueError("S3 secret key must be provided")
    return s3_key, s3_sec

def _open_root(schema_path: Path):
    s3_key, s3_sec = _get_env_vars()

    opts = {
        "S3_ACCESS_KEY": s3_key,
        "S3_SECRET_KEY": s3_sec,
        "S3_OPTIONS": json.dumps({
            "config_kwargs": {"s3": {"addressing_style": "path"}}
        }),
    }

    return zf.open_store(schema_path, **opts)


# =========================
# IO helpers
# =========================
def _atomic_write(path: Path, data: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)

def _new_msg_path(base: Path, suffix: str) -> Path:
    ts = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    uid = uuid.uuid4().hex[:12]
    return base / f"{ts}_{uid}{suffix}"

def _read_df_from_bytes(data, content_type):
    ct = content_type.lower()
    if "text/csv" in ct:
        return pl.read_csv(io.BytesIO(data))
    elif "application/json" in ct:
        return pl.read_json(io.BytesIO(data))
    else:
        raise ValueError(f"Unsupported content type: {content_type}. Use application/json or text/csv.")

def _validate_content_type(content_type):
    if not content_type:
        return False, "No Content-Type provided"
    ct = content_type.lower()
    if ("application/json" not in ct) and ("text/csv" not in ct):
        return False, f"Unsupported Content-Type: {content_type}"
    return True, None

def _sanitize_node_path(p: str) -> Path:
    p = (p or "").strip().lstrip("/")
    candidate = Path(p)

    if candidate.is_absolute() or any(part == ".." for part in candidate.parts):
        raise ValueError("Invalid node_path")

    return candidate


# =========================
# Flask handlers
# =========================
@APP.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

def _upload_node(endpoint_name, node_path=""):
    content_type = (request.headers.get("Content-Type") or "").lower()
    ok, err = _validate_content_type(content_type)
    if not ok:
        return jsonify({"error": err}), 415 if "Unsupported" in err else 400

    data = request.get_data()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    try:
        safe_child = _sanitize_node_path(node_path)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    base = (ACCEPTED_DIR / endpoint_name) / safe_child
    suffix = ".csv" if "csv" in content_type else ".json"
    msg_path = _new_msg_path(base, suffix)

    _atomic_write(msg_path, data)

    meta_data = {
        "content_type": content_type,
        "node_path": node_path,
        "endpoint_name": endpoint_name,
        "username": AUTH.current_user() or "anonymous",
        "received_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    _atomic_write(msg_path.with_suffix(msg_path.suffix + ".meta.json"), json.dumps(meta_data).encode("utf-8"))

    LOG.info("Accepted payload -> %s (user=%s)", msg_path, meta_data["username"])
    return jsonify({"status": "accepted", "path": str(msg_path.relative_to(BASE_DIR))}), 202


# =========================
# Route creation
# =========================
def _protected(view):
    return AUTH.login_required(view) if AUTH_ENABLED else view

def create_upload_endpoint(endpoint_name, endpoint_url):
    wrapped = _protected(_upload_node)

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
# Worker
# =========================
STOP = Event()

def move_tree_contents(src: Path, dst: Path):
    if not src.exists():
        return
    dst.mkdir(parents=True, exist_ok=True)

    for item in src.iterdir():
        shutil.move(str(item), str(dst / item.name))


def _iter_accepted_files():
    if not ACCEPTED_DIR.exists():
        return
    for root, _, files in os.walk(ACCEPTED_DIR):
        for name in files:
            if name.endswith(".meta.json"):
                continue
            yield Path(root) / name


def _load_meta(data_path: Path) -> dict:
    meta_path = data_path.with_suffix(data_path.suffix + ".meta.json")
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return {"content_type": "application/json", "node_path": "", "endpoint_name": data_path.parts[data_path.parts.index("accepted") + 1]}


def _target_dirs_for(data_path: Path) -> tuple[Path, Path]:
    rel = data_path.relative_to(ACCEPTED_DIR)
    return (SUCCESS_DIR / rel).parent, (FAILED_DIR / rel).parent


def _process_one(data_path: Path):
    meta = _load_meta(data_path)
    endpoint_name = meta.get("endpoint_name", "")
    node_path = meta.get("node_path", "")
    content_type = meta.get("content_type", "application/json")

    schema_path = ENDPOINT_NAME_TO_SCHEMA.get(endpoint_name)
    if not schema_path:
        raise RuntimeError(f"No schema_path mapping for endpoint_name={endpoint_name}")

    payload = data_path.read_bytes()
    df = _read_df_from_bytes(payload, content_type)

    root = _open_root(schema_path)
    if not node_path:
        root.update(df)
    else:
        root[node_path].update(df)


def _save_to_queue(src: Path, dst: Path):
    dst.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dst / src.name))

    meta = src.with_suffix(src.suffix + ".meta.json")
    if meta.exists():
        shutil.move(str(meta), str(dst / meta.name))


def working_loop(poll_sleep: float = 1.0):
    LOG.info("Worker loop started")

    while not STOP.is_set():
        progressed = False

        for data_path in list(_iter_accepted_files()):
            if STOP.is_set():
                break
            success_dir, failed_dir = _target_dirs_for(data_path)

            try:
                _process_one(data_path)
            except Exception as e:
                LOG.exception("Processing failed for %s: %s", data_path, e)
                _save_to_queue(data_path, failed_dir)
            else:
                LOG.info("Processing succeeded for %s", data_path)
                _save_to_queue(data_path, success_dir)

            progressed = True
        if not progressed:
            STOP.wait(timeout=poll_sleep)
    LOG.info("Worker loop stopped")


# =========================
# App factory & lifecycle
# =========================
def create_app():
    for ep in CONFIG.get("endpoints", []):
        create_upload_endpoint(ep["name"], ep["endpoint"])
    return APP

def _startup_recover():
    if FAILED_DIR.exists():
        LOG.info("Recovering: moving failed -> accepted")
        move_tree_contents(FAILED_DIR, ACCEPTED_DIR)

def _install_signal_handlers():
    def _on_term(signum, frame):
        LOG.info("SIGTERM received. Stopping worker…")
        STOP.set()
    try:
        signal.signal(signal.SIGTERM, _on_term)
    except Exception:
        pass


# =========================
# Main
# =========================
if __name__ == "__main__":
    create_app()
    _startup_recover()
    _install_signal_handlers()

    worker = Thread(target=working_loop, name="worker", daemon=True)
    worker.start()

    try:
        APP.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)), debug=False, use_reloader=False)
    except KeyboardInterrupt:
        LOG.info("KeyboardInterrupt – shutting down…")
    finally:
        STOP.set()
        worker.join(timeout=10)
