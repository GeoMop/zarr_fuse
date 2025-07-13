import io
import polars as pl
from flask import Blueprint, request, jsonify
from pathlib import Path
import zarr
import zarr_fuse as zf

sensors_bp = Blueprint("sensors", __name__)

SCHEMA_PATH = Path(__file__).parent / "scheme.yaml"

def get_store(store_url):
    if store_url.startswith("s3://"):
        import fsspec
        fs = fsspec.filesystem("s3")
        path = store_url[5:]
        return zarr.FSStore(path, filesystem=fs)
    elif store_url == "memory":
        return zarr.storage.MemoryStore()
    else:
        return zarr.storage.LocalStore(store_url)

def file_exists(store_url):
    if store_url.startswith("s3://") or store_url == "memory":
        return False
    path = Path(store_url)
    return path.exists() or path.is_dir()

def get_tree(store_url, schema_path=SCHEMA_PATH):
    store = get_store(store_url)
    if file_exists(store_url):
        return zf.Node.read_store(store)
    else:
        schema = zf.schema.deserialize(schema_path)
        return zf.Node("", store, new_schema=schema)

@sensors_bp.route("/", methods=["POST"])
def upload_sensors():
    if 'file' not in request.files:
        return jsonify({"error": "Missing file"}), 400

    file = request.files['file']
    store_url = request.form.get('store_url') or "sensors.zarr"

    try:
        df = pl.read_csv(io.BytesIO(file.read()))
    except Exception as e:
        return jsonify({"error": f"CSV parse error: {e}"}), 400

    try:
        tree = get_tree(store_url)
        tree.update(df)
    except Exception as e:
        import traceback
        return jsonify({
            "error": f"Update failed: {e}",
            "trace": traceback.format_exc()
        }), 500
    return jsonify({"status": "success"})
