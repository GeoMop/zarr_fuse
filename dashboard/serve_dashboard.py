import panel as pn

from composed import build_dashboard
from tile_service import S3TileHandler

pn.extension()

ROUTES = [
    (r"/tiles/([0-9]+)/([0-9]+)/([0-9]+)\.png", S3TileHandler),
]

if __name__ == "__main__":
    pn.serve(
        {"/": build_dashboard},
        address="0.0.0.0",
        port=5006,
        show=False,
        extra_patterns=ROUTES,
    )