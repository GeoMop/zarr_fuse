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

def get_store(bucket_name, file_name):
    key = os.getenv("S3_ACCESS_KEY")
    secret = os.getenv("S3_SECRET_KEY")
    endpoint = os.getenv("S3_ENDPOINT")

    if not all([key, secret, endpoint]):
        raise ValueError("S3 credentials or endpoint not set in environment variables.")

    storage_options = dict(
        key=key,
        secret=secret,
        asynchronous=False,
        client_kwargs={"endpoint_url": endpoint},
        config_kwargs={"s3": {"addressing_style": "path"}}
    )

    path = f"{bucket_name}/{file_name}"
    fs = fsspec.filesystem("s3", **storage_options)
    return zarr.storage.FsspecStore(fs, path=path)

def get_tree(bucket, filename, schema_path):
    store = get_store(bucket, filename)
    schema = zf.schema.deserialize(Path(schema_path))

    try:
        return zf.Node.read_store(store)
    except Exception:
        # fallback to create if missing
        return zf.Node("", store, new_schema=schema)

def create_upload_endpoint(app, name, endpoint_url, schema_path):
    def make_upload_node(endpoint_name):
        def upload_node(node_path=""):
            bucket = request.form.get("bucket") # take from endpoint_config.yaml
            filename = request.form.get("filename") # take from endpoint_config.yaml
            file = request.files.get("file")
            if not bucket or not filename or not file:
                return jsonify({"error": "Missing 'bucket', 'filename', or file"}), 400

            try:
                df = pl.read_csv(io.BytesIO(file.read()))
                tree = get_tree(bucket, filename, schema_path)
                if name in ["tree", "sensors", "weather"]:
                    tree.update(df)
                else:
                    node = tree[node_path]
                    node.update(df)
            except Exception as e:
                return jsonify({
                    "error": f"Update failed: {e}",
                    "trace": traceback.format_exc()
                }), 500

            return jsonify({"status": f"Updated {node_path or '/'} successfully"})

        upload_node.__name__ = f"upload_node_{endpoint_name.replace('-', '_')}"
        return upload_node

    app.route(f"{endpoint_url}", methods=["POST"])(make_upload_node(name))
    app.route(f"{endpoint_url}/<path:node_path>", methods=["POST"])(make_upload_node(name + "_sub"))

def create_app():
    app = Flask(__name__)
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
