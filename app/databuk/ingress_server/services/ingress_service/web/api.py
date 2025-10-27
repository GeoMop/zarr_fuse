import logging

from fastapi import FastAPI, Request, Depends
from fastapi.responses import JSONResponse

from packages.common import s3io, validation
from packages.common.models.metadata_model import MetadataModel
from ingress_service.web import auth

LOG = logging.getLogger("ingress")

async def _upload_node(
    endpoint_name: str,
    schema_name: str,
    request: Request,
    username: str,
    node_path: str = "",
) -> JSONResponse:
    LOG.debug(
        "ingress.request endpoint=%s node_path=%r ct=%r user=%r",
        endpoint_name,
        node_path,
        request.headers.get("content-type"),
        username,
    )

    content_type = (request.headers.get("Content-Type") or "").lower()
    ok, err = validation.validate_content_type(content_type)
    if not ok:
        LOG.warning("Validation content type failed for %s: %s", content_type, err)
        return JSONResponse({"error": err}, status_code=415 if "Unsupported" in err else 400)

    payload = await request.body()
    ok, err = validation.validate_data(payload, content_type)
    if not ok:
        LOG.warning("Validating data failed for %s", err)
        return JSONResponse({"error": err}, status_code=406)

    meta_data = MetadataModel(
        content_type=content_type,
        node_path=node_path,
        endpoint_name=endpoint_name,
        username=username,
        schema_name=schema_name,
    )

    location = s3io.save_accepted_object(endpoint_name, node_path, content_type, payload, meta_data)

    LOG.info(
        "Accepted endpoint=%s node=%s loc=%s bytes=%d",
        endpoint_name,
        node_path,
        location,
        len(payload),
    )
    return JSONResponse({"status": "accepted"}, status_code=202)

def register_upload_endpoints(app: FastAPI, endpoint_name: str, endpoint_url: str, schema_name: str):
    @app.post(
        endpoint_url,
        summary=f"Upload to root node {endpoint_name}",
        description=f"Upload data to the s3 queue for endpoint {endpoint_name}.",
        responses={
            202: {"description": "Accepted (saved to S3)"},
            400: {"description": "Bad Request"},
            406: {"description": "Not Acceptable (data validation failed)"},
            415: {"description": "Unsupported Media Type (content type not supported)"},
        }
    )
    async def upload_root(request: Request, username: str = Depends(auth.authenticate)):
        return await _upload_node(endpoint_name, schema_name, request, username)

    @app.post(
        f"{endpoint_url}/{{node_path:path}}",
        summary=f"Upload to child node of {endpoint_name}",
        description=f"Upload child data to the s3 queue for endpoint {endpoint_name}.",
        responses={
            202: {"description": "Accepted (saved to S3)"},
            400: {"description": "Bad Request"},
            406: {"description": "Not Acceptable (data validation failed)"},
            415: {"description": "Unsupported Media Type (content type not supported)"},
        }
    )
    async def upload_node(node_path: str, request: Request, username: str = Depends(auth.authenticate)):
        return await _upload_node(endpoint_name, schema_name, request, username, node_path)
