import os

from dashboard.config import load_environment_from_config, resolve_endpoints_path

# Bootstrap env loading before importing dashboard modules that read env vars at import time.
load_environment_from_config(resolve_endpoints_path())

# Backward-compatible mapping: if legacy ZF_ vars are present but S3_ are not, set them
os.environ.setdefault("S3_ACCESS_KEY", os.getenv("ZF_S3_ACCESS_KEY"))
os.environ.setdefault("S3_SECRET_KEY", os.getenv("ZF_S3_SECRET_KEY"))
os.environ.setdefault("S3_ENDPOINT_URL", os.getenv("ZF_S3_ENDPOINT_URL"))

import panel as pn
from dashboard.composed import build_dashboard
from dashboard.tile_service import S3TileHandler

pn.extension()

ROUTES = [
    (r"/tiles/([0-9]+)/([0-9]+)/([0-9]+)\.png", S3TileHandler),
]


def main() -> None:
    # Read configuration from environment variables
    bind_address = os.getenv("SERVE_BIND", "0.0.0.0")
    bind_port = int(os.getenv("SERVE_PORT", "5006"))
    allow_websocket_origin = [
        origin.strip()
        for origin in os.getenv("BOKEH_ALLOW_WS_ORIGIN", "localhost:5006").split(",")
        if origin.strip()
    ]
    
    pn.serve(
        {"/": build_dashboard, "/ui": build_dashboard},
        address=bind_address,
        port=bind_port,
        show=False,
        allow_websocket_origin=allow_websocket_origin,
        extra_patterns=ROUTES,
    )


if __name__ == "__main__":
    main()