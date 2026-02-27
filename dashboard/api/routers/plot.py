from typing import Any, Dict, Optional

from fastapi import APIRouter, Request

from api.services.data_service import DataService

router = APIRouter()


def _service(request: Request) -> DataService:
    return DataService(request.app.state.endpoints_path)


@router.post("/plot")
async def get_plot(payload: Dict[str, Any], request: Request):
    plot_type = payload.get("plot_type")
    endpoint_name = payload.get("endpoint_name")
    group_path = payload.get("group_path", "bukov")
    variable = payload.get("variable", "rock_temp")
    time_index = payload.get("time_index", 0)
    depth_index = payload.get("depth_index", 0)
    lat = payload.get("lat")
    lon = payload.get("lon")

    service = _service(request)

    if plot_type == "map":
        return service.get_map_data(
            endpoint_name,
            group_path=group_path,
            variable=variable,
            time_index=time_index,
            depth_index=depth_index,
        )

    if plot_type == "timeseries":
        if lat is None or lon is None:
            return {"status": "error", "reason": "lat/lon required for timeseries"}
        return service.get_timeseries_data(
            endpoint_name,
            group_path=group_path,
            variable=variable,
            lat=float(lat),
            lon=float(lon),
        )

    return {"status": "error", "reason": f"Unknown plot_type '{plot_type}'"}


@router.get("/structure")
async def get_structure(request: Request, endpoint: Optional[str] = None):
    service = _service(request)
    return service.get_structure(endpoint)
