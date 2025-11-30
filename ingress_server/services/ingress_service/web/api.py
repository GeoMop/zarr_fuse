import logging

from fastapi import FastAPI, Request, Depends
from fastapi.responses import JSONResponse

from . import auth
from packages.common import s3io, validation
from packages.common.models import MetadataModel
from packages.common.models import EndpointConfig

LOG = logging.getLogger("ingress")

async def _upload_node(
    endpoint_config: EndpointConfig,
    request: Request,
    username: str,
    node_path: str = "",
) -> JSONResponse:
    LOG.debug(
        "ingress.request name=%s endpoint=%s node_path=%r ct=%r user=%r",
        endpoint_config.name,
        endpoint_config.endpoint,
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
    ok, err = validation.validate_data(payload)
    if not ok:
        LOG.warning("Validating data failed for %s", err)
        return JSONResponse({"error": err}, status_code=406)

    meta_data = MetadataModel(
        content_type=content_type,
        node_path=node_path,
        endpoint_name=endpoint_config.name,
        username=username,
        schema_name=endpoint_config.schema_name,
        extract_fn=endpoint_config.extract_fn,
        fn_module=endpoint_config.fn_module,
    )

    location = s3io.save_accepted_object(endpoint_config.name, node_path, content_type, payload, meta_data)

    LOG.info(
        "Accepted name=%s endpoint=%s node=%s loc=%s bytes=%d",
        endpoint_config.name,
        endpoint_config.endpoint,
        node_path,
        location,
        len(payload),
    )
    return JSONResponse({"status": "accepted"}, status_code=202)

def register_upload_endpoints(app: FastAPI, endpoint_config: EndpointConfig):
    @app.post(
        endpoint_config.endpoint,
        summary=f"Upload to root node {endpoint_config.name}",
        description=f"Upload data to the s3 queue for endpoint {endpoint_config.name}.",
        responses={
            202: {"description": "Accepted (saved to S3)"},
            400: {"description": "Bad Request"},
            406: {"description": "Not Acceptable (data validation failed)"},
            415: {"description": "Unsupported Media Type (content type not supported)"},
        }
    )
    async def upload_root(request: Request, username: str = Depends(auth.authenticate)):
        return await _upload_node(endpoint_config, request, username)

    @app.post(
        f"{endpoint_config.endpoint}/{{node_path:path}}",
        summary=f"Upload to child node of {endpoint_config.name}",
        description=f"Upload child data to the s3 queue for endpoint {endpoint_config.name}.",
        responses={
            202: {"description": "Accepted (saved to S3)"},
            400: {"description": "Bad Request"},
            406: {"description": "Not Acceptable (data validation failed)"},
            415: {"description": "Unsupported Media Type (content type not supported)"},
        }
    )
    async def upload_node(node_path: str, request: Request, username: str = Depends(auth.authenticate)):
        return await _upload_node(endpoint_config, request, username, node_path)
