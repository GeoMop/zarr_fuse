import logging

from flask import request, jsonify

from common import validation
from common import s3io
from common.models.metadata_model import MetadataModel
from ingress_server.web.auth import AUTH

LOG = logging.getLogger("ingress")

def _upload_node(endpoint_name: str, schema_path: str, node_path: str = ""):
    LOG.debug("ingress.request endpoint=%s node_path=%r ct=%r user=%r",
        endpoint_name, node_path, request.headers.get("Content-Type"), AUTH.current_user())

    content_type = (request.headers.get("Content-Type") or "").lower()
    ok, err = validation.validate_content_type(content_type)
    if not ok:
        LOG.warning("Validation content type failed for %s: %s", content_type, err)
        return jsonify({"error": err}), 415 if "Unsupported" in err else 400

    payload = request.get_data()
    ok, err = validation.validate_data(payload, content_type)
    if not ok:
        LOG.warning("Validating data failed for %s", err)
        return jsonify({"error": err}), 400

    meta_data = MetadataModel(
        content_type=content_type,
        node_path=node_path,
        endpoint_name=endpoint_name,
        username=AUTH.current_user(),
        schema_path=schema_path,
    )

    location = s3io.save_accepted_object(endpoint_name, node_path, content_type, payload, meta_data)

    LOG.info("Accepted endpoint=%s node=%s loc=%s bytes=%d",
            endpoint_name, node_path, location, len(payload))
    return jsonify({"status": "accepted"}), 202


def register_upload_endpoints(app, endpoint_name: str, endpoint_url: str, schema_path: str):
    @app.post(endpoint_url)
    @AUTH.login_required
    def upload_root():
        return _upload_node(endpoint_name, schema_path, "")

    @app.post(f"{endpoint_url}/<path:node_path>")
    @AUTH.login_required
    def upload_node(node_path: str):
        return _upload_node(endpoint_name, schema_path, node_path)
