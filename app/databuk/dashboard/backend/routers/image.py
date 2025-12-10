from fastapi import APIRouter
from fastapi.responses import FileResponse, Response
import os

router = APIRouter()

@router.get("/api/image/{filename}")
async def get_image(filename: str):
    file_path = os.path.join("config/bukov_endpoint", filename)
    if not os.path.isfile(file_path):
        return Response(content="File not found", status_code=404)
    response = FileResponse(file_path, media_type="image/png")
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Credentials"] = "true"
    response.headers["Access-Control-Allow-Methods"] = "GET,OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    return response
