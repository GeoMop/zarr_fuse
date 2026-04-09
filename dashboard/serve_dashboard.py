import os

import panel as pn
from dotenv import load_dotenv

from composed import build_dashboard
from tile_service import S3TileHandler

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
    
    pn.serve(
        {"/": build_dashboard, "/app": build_dashboard},
        address=bind_address,
        port=bind_port,
        show=False,
        extra_patterns=ROUTES,
    )


if __name__ == "__main__":
    main()