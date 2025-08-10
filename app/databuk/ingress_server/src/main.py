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

load_dotenv()

CONFIG_PATH = Path(__file__).parent / "endpoints_config.yaml"

def get_schema_attributes(schema_path):
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_dict = yaml.safe_load(f)

    try:
        s3_url = schema_dict["ATTRS"]["s3_url"]
        store_path = schema_dict["ATTRS"]["store_path"]
    except KeyError as e:
        raise ValueError(f"Missing key in schema: {e}")

    return s3_url, store_path

def get_store(schema_path):
    s3_url, store_path = get_schema_attributes(schema_path)
    key = os.getenv("S3_ACCESS_KEY")
    secret = os.getenv("S3_SECRET_KEY")

    # TODO: do better validation
    if not all([key, secret, s3_url, store_path]):
        raise ValueError("S3 credentials and paths not set in environment variables.")

    storage_options = dict(
        key=key,
        secret=secret,
        asynchronous=False,
        client_kwargs={"endpoint_url": s3_url},
        config_kwargs={"s3": {"addressing_style": "path"}}
    )

    fs = fsspec.filesystem("s3", **storage_options)
    return zarr.storage.FsspecStore(fs, path=store_path)

def get_tree(schema_path):
    store = get_store(schema_path)
    schema = zf.schema.deserialize(Path(schema_path))

    try:
        return zf.Node.read_store(store)
    except Exception:
        # fallback to create if missing
        return zf.Node("", store, new_schema=schema)

def get_file_data(file):
    if file.filename.endswith('.csv'):
        return pl.read_csv(io.BytesIO(file.read()))
    elif file.filename.endswith('.json'):
        return pl.read_json(io.BytesIO(file.read()))
    else:
        raise ValueError("Unsupported file type. Only CSV and JSON are allowed.")

def create_upload_endpoint(app, name, endpoint_url, schema_path):
    def make_upload_node(endpoint_name):
        def upload_node(node_path=""):
            file = request.files.get("file")
            if not file:
                return jsonify({"error": "No file provided"}), 400

            try:
                df = get_file_data(file)
                tree = get_tree(schema_path)
                if name in ["tree", "sensor", "weather"]:
                    tree.update(df)
                else:
                    node = tree[node_path]
                    node.update(df)
            except Exception as e:
                return jsonify({
                    "error": f"Update failed: {e}",
                    "trace": traceback.format_exc()
                }), 400

            return jsonify({"status": f"Updated {node_path or '/'} successfully"})

        upload_node.__name__ = f"upload_node_{endpoint_name.replace('-', '_')}"
        return upload_node

    app.route(f"{endpoint_url}", methods=["POST"])(make_upload_node(name))
    app.route(f"{endpoint_url}/<path:node_path>", methods=["POST"])(make_upload_node(name + "_sub"))

def register_health_endpoint(app):
    @app.route("/health", methods=["GET"])
    def health():
        return jsonify({"status": "ok"}), 200

def create_app():
    app = Flask(__name__)
    register_health_endpoint(app)

    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)

    for endpoint in config.get("endpoints", []):
        name = endpoint["name"]
        endpoint_url = endpoint["endpoint"]
        schema_path = endpoint["schema_path"]
        create_upload_endpoint(app, name, endpoint_url, schema_path)

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(debug=True, port=8000)
