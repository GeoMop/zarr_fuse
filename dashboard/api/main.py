import os
from pathlib import Path

from fastapi import FastAPI

# Router imports commented out until implemented
# from api.routers import config as config_router
# from api.routers import config_compat as config_compat_router
# from api.routers import plot as plot_router
# from api.routers import s3 as s3_router


def create_app() -> FastAPI:
    app = FastAPI(title="HoloViz Dashboard API", version="0.1.0")
    # Router registration commented out until routers are implemented
    # app.include_router(config_router.router, prefix="/api")
    # app.include_router(plot_router.router, prefix="/api")
    # app.include_router(config_compat_router.router)
    # app.include_router(s3_router.router)
    return app


app = create_app()


# Resolve endpoints path from environment variable or packaged default
ENDPOINTS_PATH_ENV = os.getenv("ENDPOINTS_PATH")
if ENDPOINTS_PATH_ENV:
    app.state.endpoints_path = Path(ENDPOINTS_PATH_ENV)
else:
    # Fallback to packaged default
    DEFAULT_ENDPOINTS = Path(__file__).resolve().parent.parent / "config" / "endpoints.yaml"
    app.state.endpoints_path = DEFAULT_ENDPOINTS


@app.get("/health")
async def health():
    return {"status": "ok"}
