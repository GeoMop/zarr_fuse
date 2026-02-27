from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Request

from api.services.data_service import DataService

router = APIRouter(prefix="/s3", tags=["s3"])


def _service(request: Request) -> DataService:
    return DataService(request.app.state.endpoints_path)


@router.post("/plot")
async def get_plot_data(payload: Dict[str, Any], request: Request):
    try:
        plot_type = payload.get("plot_type")
        endpoint_name = payload.get("endpoint") or payload.get("endpoint_name")
        group_path = payload.get("node_path") or payload.get("group_path", "bukov")
        selection = payload.get("selection") or {}

        service = _service(request)

        if plot_type == "map":
            variable = selection.get("variable", payload.get("variable", "rock_temp"))
            time_index = selection.get("time_index", payload.get("time_index", 0))
            depth_index = selection.get("depth_index", payload.get("depth_index", 0))
            figure = service.get_map_data(
                endpoint_name,
                group_path=group_path,
                variable=variable,
                time_index=time_index,
                depth_index=depth_index,
            )
        elif plot_type == "timeseries":
            lat = selection.get("lat_point", payload.get("lat"))
            lon = selection.get("lon_point", payload.get("lon"))
            if lat is None or lon is None:
                return {"status": "error", "reason": "Missing lat_point/lon_point for timeseries"}
            variable = selection.get("variable", payload.get("variable", "rock_temp"))
            figure = service.get_timeseries_data(
                endpoint_name,
                group_path=group_path,
                variable=variable,
                lat=float(lat),
                lon=float(lon),
            )
        else:
            return {"status": "error", "reason": f"Unknown plot_type: {plot_type}"}

        return {"status": "success", "figure": figure, "overlay": None}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/structure")
async def get_store_structure(request: Request, endpoint: Optional[str] = None):
    try:
        service = _service(request)
        return service.get_structure(endpoint)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to get store structure: {exc}")


@router.get("/status")
async def get_s3_status(request: Request):
    try:
        service = _service(request)
        structure = service.get_structure(None)
        return {"status": "success", "s3_status": {"connected": True, "endpoint": None, "description": None}, "structure": structure}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to get S3 status: {exc}")
