import os
from pathlib import Path
from importlib import resources

from fastapi import FastAPI


# Router imports commented out until implemented
# from api.routers import config as config_router
# from api.routers import config_compat as config_compat_router
# from api.routers import plot as plot_router
# from api.routers import s3 as s3_router


def resolve_endpoints_path() -> Path:
    """
    Resolution order:
    1. ENDPOINTS_PATH env var
    2. Search upwards from current working directory for:
       - dashboard/config/endpoints.yaml
       - config/endpoints.yaml
    3. Packaged default inside this installed package
    """
    env_path = os.getenv("ENDPOINTS_PATH")
    if env_path:
        path = Path(env_path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"ENDPOINTS_PATH does not exist: {path}")
        return path

    cwd = Path.cwd().resolve()
    for base in [cwd, *cwd.parents]:
        candidates = [
            base / "dashboard" / "config" / "endpoints.yaml",
            base / "config" / "endpoints.yaml",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate

    try:
        packaged = resources.files("api.config").joinpath("endpoints.yaml")
        if packaged.is_file():
            return Path(packaged)
    except Exception:
        pass

    raise FileNotFoundError(
        "Could not find endpoints.yaml. Checked:\n"
        "1. ENDPOINTS_PATH env var\n"
        "2. dashboard/config/endpoints.yaml\n"
        "3. config/endpoints.yaml\n"
        "4. packaged default api/config/endpoints.yaml"
    )


def create_app() -> FastAPI:
    app = FastAPI(title="HoloViz Dashboard API", version="0.1.0")

    # Router registration commented out until routers are implemented
    # app.include_router(config_router.router, prefix="/api")
    # app.include_router(plot_router.router, prefix="/api")
    # app.include_router(config_compat_router.router)
    # app.include_router(s3_router.router)

    app.state.endpoints_path = resolve_endpoints_path()
    return app


app = create_app()


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "endpoints_path": str(app.state.endpoints_path),
    }