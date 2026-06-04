from fastapi import FastAPI

from dashboard.config import resolve_endpoints_path


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

    app.state.endpoints_path = resolve_endpoints_path()
    return app


app = create_app()


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "endpoints_path": str(app.state.endpoints_path),
    }