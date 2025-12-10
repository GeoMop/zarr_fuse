import json
import numpy as np

# PNG dimensions (update if needed)
width = 8742
height = 8025

# Load GCP points from raw JSON
with open("config/surface_data_endpoint/uhelna_georef.raw.json", "r", encoding="utf-8") as f:
    data = json.load(f)

src_pts = []
dst_pts = []
for pt in data["points"]:
    if pt.get("enable", True):
        src_pts.append([pt["sourceX"], abs(pt["sourceY"])] )
        dst_pts.append([pt["mapX"], pt["mapY"]])  # [Lon, Lat]

src = np.array(src_pts)
dst = np.array(dst_pts)

# Affine transformation matrix
A = np.c_[src, np.ones(len(src))]
transform_matrix, _, _, _ = np.linalg.lstsq(A, dst, rcond=None)

# PNG corners
corners_px = np.array([
    [0, 0, 1],          # Top-Left
    [width, 0, 1],      # Top-Right
    [width, height, 1], # Bottom-Right
    [0, height, 1]      # Bottom-Left
])

corners_geo = corners_px @ transform_matrix  # [Lon, Lat]

lons = corners_geo[:, 0]
lats = corners_geo[:, 1]
minLon, maxLon = lons.min(), lons.max()
minLat, maxLat = lats.min(), lats.max()
map_bounds = [ [minLon, minLat], [maxLon, maxLat] ]

print("Map raster overlay bounds:", map_bounds)
