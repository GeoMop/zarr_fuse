import json
import numpy as np
import os

def get_overlay_corners():
    """
    Returns PNG overlay corner coordinates and bounds using affine transformation.
    """
    width = 8742
    height = 8025
    gcp_path = os.path.join(
        os.path.dirname(__file__),
        "../config/surface_data_endpoint/uhelna_georef.raw.json"
    )
    with open(gcp_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    src_pts = []
    dst_pts = []
    for pt in data["points"]:
        if pt.get("enable", True):
            src_pts.append([pt["sourceX"], abs(pt["sourceY"])] )
            dst_pts.append([pt["mapX"], pt["mapY"]])
    src = np.array(src_pts)
    dst = np.array(dst_pts)
    # Affine transformation matrix
    A = np.c_[src, np.ones(len(src))]
    transform_matrix, _, _, _ = np.linalg.lstsq(A, dst, rcond=None)
    # PNG corners in pixel coordinates
    corners_px = np.array([
        [0, 0, 1],          # Top-Left
        [width, 0, 1],      # Top-Right
        [width, height, 1], # Bottom-Right
        [0, height, 1]      # Bottom-Left
    ])
    corners_geo = corners_px @ transform_matrix
    corners = corners_geo.tolist()
    lons = corners_geo[:, 0]
    lats = corners_geo[:, 1]
    minLon, maxLon = lons.min(), lons.max()
    minLat, maxLat = lats.min(), lats.max()
    bounds = [ [float(minLon), float(minLat)], [float(maxLon), float(maxLat)] ]
    overlay = {
        "bounds": bounds,
        "corners": [
            [float(corners[0][0]), float(corners[0][1])], # top-left
            [float(corners[1][0]), float(corners[1][1])], # top-right
            [float(corners[2][0]), float(corners[2][1])], # bottom-right
            [float(corners[3][0]), float(corners[3][1])]  # bottom-left
        ]
    }
    return overlay
