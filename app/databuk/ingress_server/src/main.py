from email import header
import io
import yaml
import os
import traceback
from pathlib import Path
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import polars as pl
import zarr
import zarr_fuse as zf
import fsspec
import json
from flask_httpauth import HTTPBasicAuth


# ---------- BOOTSTRAPPING ----------
load_dotenv()
CONFIG_PATH = Path(__file__).parent / "endpoints_config.yaml"
USERS = json.loads(os.getenv("USERS_JSON", "{}"))
AUTH = HTTPBasicAuth()
APP = Flask(__name__)


# ---------- AUTH ----------
@AUTH.verify_password
def verify_password(username, password):
    # use check password hash
    if username in USERS and USERS[username] == password:
        return username
    return None


# ---------- S3 HELPERS ----------
def _get_schema_attributes(schema_path):
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_dict = yaml.safe_load(f)

    try:
        s3_url = schema_dict["ATTRS"]["s3_url"]
        store_path = schema_dict["ATTRS"]["store_path"]
    except KeyError as e:
        raise ValueError(f"Missing key in schema: {e}")

    return s3_url, store_path

def _store_params_validation(s3_url, store_path, key, secret):
    if s3_url is None:
        raise ValueError("S3 URL must be provided")
    if store_path is None:
        raise ValueError("Store path must be provided")
    if key is None:
        raise ValueError("S3 access key must be provided")
    if secret is None:
        raise ValueError("S3 secret key must be provided")

def _get_store(schema_path):
    s3_url, store_path = _get_schema_attributes(schema_path)
    key = os.getenv("S3_ACCESS_KEY")
    secret = os.getenv("S3_SECRET_KEY")

    _store_params_validation(s3_url, store_path, key, secret)

    storage_options = dict(
        key=key,
        secret=secret,
        asynchronous=False,
        client_kwargs={"endpoint_url": s3_url},
        config_kwargs={"s3": {"addressing_style": "path"}}
    )

    fs = fsspec.filesystem("s3", **storage_options)
    return zarr.storage.FsspecStore(fs, path=store_path)

def _get_tree(schema_path):
    store = _get_store(schema_path)
    schema = zf.schema.deserialize(Path(schema_path))

    try:
        return zf.Node.read_store(store)
    except Exception:
        return zf.Node("", store, new_schema=schema)


# ---------- DATA PARSING ----------
def _normalize_content_type(value: str | None) -> str:
    if not value:
        return ""
    return value.split(";", 1)[0].strip().lower()

def _get_file_data(data, content_type):
    type = _normalize_content_type(content_type)
    if type == "text/csv":
        return pl.read_csv(io.BytesIO(data))
    elif type == "application/json":
        return pl.read_json(io.BytesIO(data))
    else:
        raise ValueError("Unsupported content type. Use application/json or text/csv.")


# ---------- HANDLERS ----------
@AUTH.login_required
def upload_node(schema_path, node_path=""):
    content_type = (request.headers.get("Content-Type") or "").lower()
    data = request.data

    if not data:
        return jsonify({"error": "No file provided"}), 400
    if not content_type:
        return jsonify({"error": "No content type provided"}), 400

    try:
        df = _get_file_data(data, content_type)
        tree = _get_tree(schema_path)
        if not node_path:
            tree.update(df)
        else:
            node = tree[node_path]
            node.update(df)
    except Exception as e:
        return jsonify({"error": f"Update failed: {e}", "trace": traceback.format_exc()}), 400

    return jsonify({"status": f"Updated {node_path or '/'} successfully"})


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

    with open(CONFIG_PATH, "r") as f:
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
