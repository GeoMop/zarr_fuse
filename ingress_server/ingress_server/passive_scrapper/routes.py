import logging
from collections.abc import Callable
from typing import Awaitable

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from .auth import verify_basic_auth
from ..models import EndpointConfig
from ..io import process_payload
from ..app_config import AppConfig

LOG = logging.getLogger(__name__)


async def _upload_node(
    request: Request,
    endpoint_config: EndpointConfig,
    app_config: AppConfig,
    node_path: str,
    user: str,
) -> JSONResponse:
    content_type = (request.headers.get("content-type") or "").lower()
    data = await request.body()

    LOG.debug(
        "Received upload request endpoint=%s node_path=%r content_type=%r user=%r bytes=%s",
        endpoint_config.name,
        node_path,
        content_type,
        user,
        len(data),
    )

    try:
        process_payload(
            app_config=app_config,
            data_source=endpoint_config.data_source,
            payload=data,
            content_type=content_type,
            username=user,
            node_path=node_path,
        )
    except ValueError as exc:
        LOG.warning(
            "Rejected payload for endpoint=%s node_path=%r user=%r: %s",
            endpoint_config.name,
            node_path,
            user,
            exc,
        )
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception:
        LOG.exception(
            "Unexpected processing failure for endpoint=%s node_path=%r user=%r",
            endpoint_config.name,
            node_path,
            user,
        )
        raise HTTPException(status_code=500, detail="Internal processing error")

    LOG.info(
        "Accepted upload for endpoint=%s node_path=%r user=%r content_type=%r bytes=%s",
        endpoint_config.name,
        node_path,
        user,
        content_type,
        len(data),
    )

    return JSONResponse({"status": "accepted"}, status_code=202)


def make_upload_handler(
    app_config: AppConfig,
    endpoint_config: EndpointConfig,
) -> Callable[..., Awaitable[JSONResponse]]:
    async def upload_handler(
        request: Request,
        node_path: str = "",
        user: str = Depends(verify_basic_auth),
    ) -> JSONResponse:
        return await _upload_node(
            request=request,
            endpoint_config=endpoint_config,
            app_config=app_config,
            node_path=node_path,
            user=user,
        )

    return upload_handler


def register_passive_scrapper(
    app: FastAPI,
    app_config: AppConfig,
    endpoint_config: EndpointConfig,
) -> None:
    handler = make_upload_handler(app_config, endpoint_config)
    base_path = endpoint_config.endpoint.rstrip("/")

    app.add_api_route(
        base_path,
        handler,
        methods=["POST"],
        name=f"upload_node_root_{endpoint_config.name.replace('-', '_')}",
    )

    app.add_api_route(
        f"{base_path}/{{node_path:path}}",
        handler,
        methods=["POST"],
        name=f"upload_node_sub_{endpoint_config.name.replace('-', '_')}",
    )

    LOG.info(
        "Created upload endpoint name=%s path=%s",
        endpoint_config.name,
        base_path,
    )
