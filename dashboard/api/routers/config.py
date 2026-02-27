from pathlib import Path
import sys

from fastapi import APIRouter, Request

CONFIG_ROOT = Path(__file__).resolve().parents[3] / "app" / "databuk" / "dashboard" / "holoviz_dashboard"
if str(CONFIG_ROOT) not in sys.path:
    sys.path.insert(0, str(CONFIG_ROOT))

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
