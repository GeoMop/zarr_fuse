import io
import yaml
import os
import traceback
from pathlib import Path
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import polars as pl
import zarr_fuse as zf
import json
from flask_httpauth import HTTPBasicAuth
import time, uuid, shutil

# ---------- DEFAULT BOOTSTRAPPING ----------
load_dotenv()
APP = Flask(__name__)


# ---------- QUEUE BOOTSTRAPPING ----------
QUEUE_DIR = Path(os.getenv("QUEUE_DIR", "./data/queue"))
PENDING_DIR = QUEUE_DIR / "pending"
PROCESSED_DIR = QUEUE_DIR / "processed"
for d in (PENDING_DIR, PROCESSED_DIR):
    d.mkdir(parents=True, exist_ok=True)


# ---------- AUTH BOOTSTRAPPING ----------
def _parse_users_json(raw: str | None) -> dict:
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"[WARN] BASIC_AUTH_USERS_JSON invalid: {e}; value={raw!r}")
        return {}

AUTH = HTTPBasicAuth()
USERS = _parse_users_json(os.getenv("BASIC_AUTH_USERS_JSON"))
AUTH_ENABLED = os.getenv("AUTH_ENABLED", "true").lower() == "true"


# ---------- AUTH HELPERS ----------
@AUTH.verify_password
def verify_password(username, password):
    if not AUTH_ENABLED:
        return username

    if username in USERS and USERS[username] == password:
        return username
    return None


# ---------- QUEUE HELPERS ----------
def _atomic_write(path: Path, data: bytes):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)

def _new_msg_path(suffix: str = ".json") -> Path:
    ts = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    uid = uuid.uuid4().hex[:12]
    return PENDING_DIR / f"{ts}_{uid}{suffix}"


# ---------- S3 HELPERS ----------
def _load_schema_dict(schema_path: str | Path) -> dict:
    with open(schema_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _get_schema_attributes(schema_path):
    schema_dict = _load_schema_dict(schema_path)
    attrs = schema_dict.get("ATTRS", {}) if isinstance(schema_dict, dict) else {}

    store_url = attrs.get("STORE_URL")
    s3_endpoint = attrs.get("S3_ENDPOINT_URL")
    if s3_endpoint is None:
        raise ValueError("S3 URL must be provided")
    if store_url is None:
        raise ValueError("Store path must be provided")

    return s3_endpoint, store_url

def _get_env_vars():
    s3_key = os.getenv("S3_ACCESS_KEY")
    s3_sec = os.getenv("S3_SECRET_KEY")

    if s3_key is None:
        raise ValueError("S3 access key must be provided")
    if s3_sec is None:
        raise ValueError("S3 secret key must be provided")
    return s3_key, s3_sec

def _get_root(schema_path: Path):
    s3_ep, store_url = _get_schema_attributes(schema_path)
    s3_key, s3_sec = _get_env_vars()

    opts = {
        "STORE_URL": store_url,
        "S3_ENDPOINT_URL": s3_ep,
        "S3_ACCESS_KEY": s3_key,
        "S3_SECRET_KEY": s3_sec,
        "S3_OPTIONS": json.dumps({
            "asynchronous": True,
            "config_kwargs": {"s3": {"addressing_style": "path"}}
        }),
    }

    return zf.open_store(schema_path, **opts)

# ---------- DATA PARSING ----------
def _get_file_data(data, content_type):
    ct = (content_type or "").lower()
    if "text/csv" in ct:
        return pl.read_csv(io.BytesIO(data))
    elif "application/json" in ct:
        return pl.read_json(io.BytesIO(data))
    else:
        raise ValueError(f"Unsupported content type: {content_type}. Use application/json or text/csv.")

def _validate_request_data(data, content_type):
    if not data:
        return jsonify({"error": "No data provided"}), 400
    if not content_type:
        return jsonify({"error": "No Content-Type provided"}), 400

    ct = content_type.lower()
    if ("application/json" not in ct) and ("text/csv" not in ct):
        return jsonify({"error": f"Unsupported Content-Type: {content_type}"}), 415

    return None

# ---------- HANDLERS ----------
@AUTH.login_required
def upload_node(schema_path, node_path=""):
    content_type = (request.headers.get("Content-Type") or "").lower()
    data = request.data
    msg_path = None

    valid = _validate_request_data(data, content_type)
    if valid:
        return valid

    try:
        suffix = ".csv" if "csv" in content_type else ".json"
        msg_path = _new_msg_path(suffix)
        _atomic_write(msg_path, data)

        df = _get_file_data(data, content_type)
        root = _get_root(Path(schema_path))
        if not node_path:
            root.update(df)
        else:
            root[node_path].update(df)

        shutil.move(msg_path, PROCESSED_DIR / msg_path.name)

        return jsonify({"status": f"Updated {node_path or '/'} successfully"})

    except Exception as e:
        payload = {"error": f"Update failed: {e}", "trace": traceback.format_exc()}
        if msg_path is not None:
            payload["pending_file"] = str(msg_path)
        return jsonify(payload), 400


# ---------- ROUTE REGISTRACE ----------
def create_upload_endpoint(endpoint_name, endpoint_url, schema_path):
    # Root path (without node_path)
    APP.add_url_rule(
        endpoint_url,
        endpoint=f"upload_node_root_{endpoint_name.replace('-', '_')}",
        view_func=upload_node,
        methods=["POST"],
        defaults={"schema_path": schema_path, "node_path": ""},
    )

    # Subpath with node_path
    APP.add_url_rule(
        f"{endpoint_url}/<path:node_path>",
        endpoint=f"upload_node_sub_{endpoint_name.replace('-', '_')}",
        view_func=upload_node,
        methods=["POST"],
        defaults={"schema_path": schema_path},
    )


# ---------- HEALTH ENDPOINTS ----------
@APP.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


# ---------- APP FACTORY ----------
def create_app():
    config_path = Path(__file__).parent
    if os.getenv("PRODUCTION", "false").lower() == "true":
        config_path /= "prod_endpoints_config.yaml"
    else:
        config_path /= "ci_test_endpoints_config.yaml"

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    for endpoint in config.get("endpoints", []):
        endpoint_name = endpoint["name"]
        endpoint_url = endpoint["endpoint"]
        schema_path = endpoint["schema_path"]
        create_upload_endpoint(endpoint_name, endpoint_url, schema_path)

    return APP


# ---------- MAIN ----------
if __name__ == "__main__":
    create_app().run(debug=True, port=8000)
