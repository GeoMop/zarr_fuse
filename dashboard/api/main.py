from pathlib import Path

from fastapi import FastAPI

from api.routers import config as config_router
from api.routers import config_compat as config_compat_router
from api.routers import plot as plot_router
from api.routers import s3 as s3_router


def create_app() -> FastAPI:
    app = FastAPI(title="HoloViz Dashboard API", version="0.1.0")
    app.include_router(config_router.router, prefix="/api")
    app.include_router(plot_router.router, prefix="/api")
    app.include_router(config_compat_router.router)
    app.include_router(s3_router.router)
    return app


app = create_app()

REPO_ROOT = Path(__file__).resolve().parents[2]
app.state.endpoints_path = (
    REPO_ROOT
    / "app"
    / "databuk"
    / "dashboard"
    / "holoviz_dashboard"
    / "config"
    / "endpoints.yaml"
)


@app.get("/health")
async def health():
    return {"status": "ok"}
