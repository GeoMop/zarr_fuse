import json
from pathlib import Path
import subprocess
import sys


def build_gcp_args(points):
    args = []
    for pt in points:
        if not pt.get("enable", True):
            continue

        pixel_x = float(pt["sourceX"])
        pixel_y = abs(float(pt["sourceY"]))
        lon = float(pt["mapX"])
        lat = float(pt["mapY"])

        args.extend(["-gcp", str(pixel_x), str(pixel_y), str(lon), str(lat)])
    return args


def main():
    if len(sys.argv) < 4:
        raise ValueError(
            "Usage: python prepare_gcps.py <endpoint_dir> <georef_filename> <image_filename> [output_vrt_filename]"
        )

    endpoint_dir = Path(sys.argv[1]).resolve()
    georef_path = endpoint_dir / sys.argv[2]
    image_path = endpoint_dir / sys.argv[3]
    vrt_filename = sys.argv[4] if len(sys.argv) > 4 else "overlay_gcps.vrt"
    vrt_path = endpoint_dir / vrt_filename

    if not endpoint_dir.is_dir():
        raise FileNotFoundError(f"Missing endpoint directory: {endpoint_dir}")
    if not georef_path.is_file():
        raise FileNotFoundError(f"Missing georef file: {georef_path}")
    if not image_path.is_file():
        raise FileNotFoundError(f"Missing image file: {image_path}")

    with georef_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    points = data.get("points", [])
    enabled_points = [pt for pt in points if pt.get("enable", True)]

    if len(enabled_points) < 3:
        raise ValueError(f"Need at least 3 enabled control points, found {len(enabled_points)}")

    gcp_args = build_gcp_args(enabled_points)

    cmd = [
        "gdal_translate",
        "-of", "VRT",
        "-a_srs", "EPSG:4326",
        *gcp_args,
        str(image_path),
        str(vrt_path),
    ]

    print("Running:")
    print(" ".join(cmd))
    subprocess.run(cmd, check=True)

    print(f"\nCreated: {vrt_path}")
    print(f"Enabled GCPs: {len(enabled_points)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)