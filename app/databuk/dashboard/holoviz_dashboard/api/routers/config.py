from fastapi import APIRouter, Request

from config.dashboard_config import get_endpoint_config, load_endpoints

router = APIRouter()


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.get("/endpoints")
async def list_endpoints(request: Request):
    endpoints_path = request.app.state.endpoints_path
    endpoints = load_endpoints(endpoints_path)
    return {
        "status": "success",
        "endpoints": {name: endpoint.__dict__ for name, endpoint in endpoints.items()},
    }


@router.get("/endpoints/{endpoint_name}")
async def get_endpoint(request: Request, endpoint_name: str):
    endpoints_path = request.app.state.endpoints_path
    endpoint = get_endpoint_config(endpoints_path, endpoint_name)
    return {"status": "success", "endpoint": endpoint.__dict__}
