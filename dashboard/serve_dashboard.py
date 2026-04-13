import os

import panel as pn
from dotenv import load_dotenv

from dashboard.composed import build_dashboard
from dashboard.tile_service import S3TileHandler

# Load environment variables from .env file if present
load_dotenv()

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
        {"/": build_dashboard, "/app": build_dashboard},
        address=bind_address,
        port=bind_port,
        show=False,
        allow_websocket_origin=allow_websocket_origin,
        extra_patterns=ROUTES,
    )


if __name__ == "__main__":
    main()