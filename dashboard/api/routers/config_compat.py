from pathlib import Path
import sys

from fastapi import APIRouter, HTTPException, Request

CONFIG_ROOT = Path(__file__).resolve().parents[3] / "app" / "databuk" / "dashboard" / "holoviz_dashboard"
if str(CONFIG_ROOT) not in sys.path:
    sys.path.insert(0, str(CONFIG_ROOT))

from config.dashboard_config import get_endpoint_config, load_endpoints

router = APIRouter(prefix="/config", tags=["configuration"])


@router.get("/endpoints")
async def get_endpoints(request: Request):
    try:
        endpoints_path = request.app.state.endpoints_path
        endpoints = load_endpoints(endpoints_path)
        return {
            "status": "success",
            "endpoints": {name: endpoint.__dict__ for name, endpoint in endpoints.items()},
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to load endpoints: {exc}")


@router.get("/endpoints/{endpoint_name}")
async def get_endpoint(request: Request, endpoint_name: str):
    try:
        endpoints_path = request.app.state.endpoints_path
        endpoint = get_endpoint_config(endpoints_path, endpoint_name)
        return {"status": "success", "endpoint": endpoint.__dict__}
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/current")
async def get_current_endpoint(request: Request):
    endpoints_path = request.app.state.endpoints_path
    endpoints = load_endpoints(endpoints_path)
    if not endpoints:
        raise HTTPException(status_code=404, detail="No endpoints configured")
    endpoint = next(iter(endpoints.values()))
    return {"status": "success", "endpoint": endpoint.__dict__}


@router.post("/reload")
async def reload_config(request: Request):
    try:
        endpoints_path = request.app.state.endpoints_path
        endpoints = load_endpoints(endpoints_path)
        return {
            "status": "success",
            "message": "Configuration reloaded successfully",
            "endpoints_count": len(endpoints),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to reload configuration: {exc}")
