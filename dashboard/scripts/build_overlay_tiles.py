"""Ensure overlay map tiles exist on S3, building them first if needed.

Standalone, operator-triggered script (local terminal use). Merged
replacement for ``prepare_bukov_gcps.py`` plus the manual GDAL commands
documented in ``dashboard/docs/tile_pyramid_README.md`` plus
``dashboard/test/upload_s3.py``.

The script implements an ensure-semantics: local tile creation and S3 upload
are one nested operation - either the tile system already exists on S3 and
nothing happens, or it is built locally and uploaded in the same run.

Flow:

1. Existence check on ``s3://bucket/prefix``: if any object exists under the
   prefix, the tile system counts as present and the run ends immediately.
   GDAL is never touched. There is no staleness detection; use ``--force``
   to rebuild deliberately. ``--force`` bypasses both the S3 short-circuit
   and local output skips (full redo; unchanged tiles are still not
   re-transferred thanks to size comparison).
2. Local build when missing, then upload of all tiles in the same run.

All parameters are wired to the project configuration:

- the zf_view.yaml file itself is located like in every other dashboard
  entry point (``find_view_file``, as used by composed.py): ``--view-path``
  flag, then ``ZF_VIEW_PATH`` env, then an upward search from the current
  directory;
- the view is selected via ``--view`` / ``HV_DASHBOARD_VIEW``, falling back to
  ``_dashboard.default_endpoint``;
- build parameters (paths, zoom range, CRS, resampling, S3 bucket/prefix)
  come from the ``tile_build`` section of the selected view;
- generic processing defaults (zoom range, CRS values, resampling) are owned
  by the config layer (``TileBuildConfig`` in ``dashboard/config.py``); the
  S3 bucket/prefix target must be configured explicitly in zf_view.yaml
  (``tile_build.s3``) or given per-run via ``--bucket``/``--prefix``;
- credentials come from the general environment (``ZF_S3_ACCESS_KEY``,
  ``ZF_S3_SECRET_KEY``), filled from the ``_dashboard.env_file`` referenced by
  zf_view.yaml;
- the S3 endpoint URL is owned by the view schema (``ATTRS.S3_ENDPOINT_URL``);
  ``--endpoint-url`` can override it manually.

CLI flags always win over config values. The startup summary shows where
each value came from.

Local pipeline:

1. gdal_translate -of VRT -a_srs <gcp_srs> -gcp ... <image> <vrt>
2. gdalwarp -t_srs <target_srs> -r <resampling> -dstalpha ... <vrt> <tif>
3. gdal_translate -of VRT -expand rgba <tif> <rgba_vrt>
4. gdal2tiles --xyz -z <min>-<max> <rgba_vrt> <tiles_dir>
5. upload tiles to s3://<bucket>/<prefix><z/x/y>.png

Steps whose outputs already exist locally are skipped unless ``--force`` is
given, so an interrupted run can resume. Upload compares object sizes and
transfers only changed files.

The preprocessing requires GDAL command line tools on PATH (gdal_translate,
gdalwarp) and a working gdal2tiles (preferred: python module ``osgeo_utils``
in the current interpreter, fallback: ``gdal2tiles`` executable on PATH).
Use ``--dry-run`` to preview the planned flow without contacting S3 or
needing any tool installed.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from dashboard.config import (
        find_view_file,
        get_default_endpoint_name,
        load_environment_from_config,
        load_view_config,
        schema_endpoint_url,
    )
except ModuleNotFoundError as exc:
    raise SystemExit(
        f"ERROR: cannot import dashboard.config ({exc}).\n"
        "Run this script with the repository virtualenv python, or install\n"
        "the missing dependency into the current environment, e.g.:\n"
        "  pip install pyyaml python-dotenv"
    )

def _resolve_views_path(explicit: Optional[str]) -> tuple[Path, str]:
    """Locate zf_view.yaml: CLI flag, then the shared config finder.

    The finder (dashboard.config.find_view_file, same entry point as
    composed.py) resolves ZF_VIEW_PATH first, then searches upward from the
    current directory for zf_view.yaml candidates.
    """
    if explicit:
        path = Path(explicit).expanduser().resolve()
        if not path.is_file():
            raise SystemExit(f"ERROR: views file does not exist: {path}")
        return path, "flag"
    try:
        return find_view_file(), "env/search"
    except FileNotFoundError:
        raise SystemExit(
            "ERROR: could not locate zf_view.yaml. Set ZF_VIEW_PATH, pass "
            "--view-path, or run from inside the repository."
        )


def _require_tool(tool: str, dry_run: bool = False) -> str:
    """Return the resolved path of a required executable or exit with a hint."""
    if dry_run:
        return tool
    resolved = shutil.which(tool)
    if resolved is None:
        raise SystemExit(
            f"ERROR: required GDAL tool '{tool}' not found on PATH.\n"
            "Run this script from an environment with GDAL installed, e.g.:\n"
            "  conda activate gdal-test"
        )
    return resolved


def _has_module(name: str) -> bool:
    """Return True if the module is importable in the current interpreter."""
    try:
        return importlib.util.find_spec(name) is not None
    except (ImportError, ValueError):
        # find_spec raises ModuleNotFoundError when a parent package of a
        # dotted name is missing (e.g. osgeo without GDAL bindings).
        return False


def _resolve_gdal2tiles_cmd(dry_run: bool) -> list[str]:
    """Return the command prefix used to invoke gdal2tiles."""
    has_python_module = (
        _has_module("osgeo_utils") or _has_module("osgeo.utils")
    )
    if has_python_module or dry_run:
        return [sys.executable, "-m", "osgeo_utils.gdal2tiles"]
    return [_require_tool("gdal2tiles")]


def _run(cmd: list[str], dry_run: bool) -> None:
    """Echo the command and execute it unless dry_run."""
    print(f"Running: {' '.join(cmd)}")
    if not dry_run:
        subprocess.run(cmd, check=True)


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Build overlay XYZ tiles from a georeferenced image and upload them to S3.",
    )
    parser.add_argument("--view-path", default=None,
                        help="path to zf_view.yaml (default: ZF_VIEW_PATH env, "
                             "then upward search from the current directory)")
    parser.add_argument("--view", default=None,
                        help="view name in zf_view.yaml (default: HV_DASHBOARD_VIEW "
                             "env, then _dashboard.default_endpoint)")
    parser.add_argument("--image", default=None,
                        help="override source_image from the view config")
    parser.add_argument("--georef", default=None,
                        help="override georef_file from the view config")
    parser.add_argument("--min-zoom", type=int, default=None,
                        help="override min_zoom from the view config")
    parser.add_argument("--max-zoom", type=int, default=None,
                        help="override max_zoom from the view config")
    parser.add_argument("--gcp-srs", default=None,
                        help="override gcp_srs from the view config")
    parser.add_argument("--target-srs", default=None,
                        help="override target_srs from the view config")
    parser.add_argument("--resampling", default=None,
                        help="override resampling from the view config")
    parser.add_argument("--bucket", default=None,
                        help="override tile_build.s3.bucket from the view config")
    parser.add_argument("--prefix", default=None,
                        help="override tile_build.s3.prefix from the view config")
    parser.add_argument("--endpoint-url", default=None,
                        help="override ATTRS.S3_ENDPOINT_URL from the view schema")
    parser.add_argument("--force", action="store_true",
                        help="full redo: bypass the S3 existence short-circuit "
                             "and rebuild all local steps even if outputs exist")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the planned flow without contacting S3 or "
                             "executing any command")
    return parser.parse_args(argv)


def build_gcp_args(points: list[dict[str, Any]]) -> list[str]:
    """Convert enabled georef points into gdal_translate -gcp arguments."""
    args: list[str] = []
    for pt in points:
        if not pt.get("enable", True):
            continue
        pixel_x = float(pt["sourceX"])
        pixel_y = abs(float(pt["sourceY"]))
        lon = float(pt["mapX"])
        lat = float(pt["mapY"])
        args.extend(["-gcp", str(pixel_x), str(pixel_y), str(lon), str(lat)])
    return args


def load_georef_points(georef_path: Path) -> list[dict[str, Any]]:
    """Read the QGIS-style georef JSON and return its enabled control points."""
    with georef_path.open("r", encoding="utf-8-sig") as f:
        data = json.load(f)
    points = [pt for pt in data.get("points", []) if pt.get("enable", True)]
    if len(points) < 3:
        raise SystemExit(
            f"ERROR: need at least 3 enabled control points, found {len(points)} in {georef_path}"
        )
    return points


def _resolve_param(cli_value: Any, config_value: Any, builtin: Any) -> tuple[Any, str]:
    """Resolve one parameter: CLI flag beats config value beats builtin default."""
    if cli_value is not None:
        return cli_value, "flag"
    if config_value is not None:
        return config_value, "config"
    return builtin, "default"


def step_build_gcps_vrt(image_path: Path, georef_path: Path, vrt_path: Path,
                        gcp_srs: str, force: bool, dry_run: bool) -> bool:
    """Step 1: attach GCPs to the source image, producing a VRT. Returns True if run."""
    if vrt_path.exists() and not force:
        print(f"[2/6] skip GCP VRT (exists): {vrt_path}")
        return False
    points = load_georef_points(georef_path)
    cmd = [
        _require_tool("gdal_translate", dry_run),
        "-of", "VRT",
        "-a_srs", gcp_srs,
        *build_gcp_args(points),
        str(image_path),
        str(vrt_path),
    ]
    print(f"[2/6] GCP VRT ({len(points)} control points)")
    _run(cmd, dry_run)
    return True


def step_warp(vrt_path: Path, tif_path: Path,
              target_srs: str, resampling: str, force: bool, dry_run: bool) -> bool:
    """Step 2: warp the GCP VRT into the target SRS GeoTIFF. Returns True if run."""
    if tif_path.exists() and not force:
        print(f"[3/6] skip warp (exists): {tif_path}")
        return False
    cmd = [
        _require_tool("gdalwarp", dry_run),
        "-t_srs", target_srs,
        "-r", resampling,
        "-dstalpha",
        "-overwrite",
        "-of", "GTiff",
        "-co", "TILED=YES",
        "-co", "COMPRESS=DEFLATE",
        str(vrt_path),
        str(tif_path),
    ]
    print("[3/6] warp to web mercator")
    _run(cmd, dry_run)
    return True


def step_expand_rgba(tif_path: Path, rgba_vrt_path: Path,
                     force: bool, dry_run: bool) -> bool:
    """Step 3: expand the warped raster to RGBA VRT. Returns True if run."""
    if rgba_vrt_path.exists() and not force:
        print(f"[4/6] skip RGBA expansion (exists): {rgba_vrt_path}")
        return False
    cmd = [
        _require_tool("gdal_translate", dry_run),
        "-of", "VRT",
        "-expand", "rgba",
        str(tif_path),
        str(rgba_vrt_path),
    ]
    print("[4/6] RGBA expansion")
    _run(cmd, dry_run)
    return True


def step_generate_tiles(rgba_vrt_path: Path, tiles_dir: Path,
                        min_zoom: int, max_zoom: int, force: bool, dry_run: bool) -> bool:
    """Step 4: generate the XYZ tile pyramid. Returns True if run."""
    if tiles_dir.exists() and not force:
        print(f"[5/6] skip tile generation (exists): {tiles_dir}")
        return False
    cmd = [
        *_resolve_gdal2tiles_cmd(dry_run),
        "--xyz",
        "-z", f"{min_zoom}-{max_zoom}",
        str(rgba_vrt_path),
        str(tiles_dir),
    ]
    print(f"[5/6] XYZ tiles zoom {min_zoom}-{max_zoom}")
    _run(cmd, dry_run)
    return True


def collect_tile_files(tiles_dir: Path) -> list[tuple[Path, str]]:
    """Return (local_path, relative_posix_key) pairs for all PNG tiles."""
    pairs: list[tuple[Path, str]] = []
    for root, _dirs, files in os.walk(tiles_dir):
        for name in files:
            if not name.lower().endswith(".png"):
                continue
            local_path = Path(root) / name
            rel = local_path.relative_to(tiles_dir).as_posix()
            pairs.append((local_path, rel))
    pairs.sort(key=lambda pair: pair[1])
    return pairs


def resolve_upload_config(endpoint_url: Optional[str]) -> dict[str, Optional[str]]:
    """Collect S3 credentials from the general environment."""
    config: dict[str, Optional[str]] = {
        "access_key": os.getenv("ZF_S3_ACCESS_KEY"),
        "secret_key": os.getenv("ZF_S3_SECRET_KEY"),
        "endpoint_url": endpoint_url,
    }
    missing = [name for name in ("access_key", "secret_key") if not config[name]]
    if missing:
        raise SystemExit(
            "ERROR: missing S3 credentials: "
            + ", ".join(missing)
            + "\nProvide them as environment variables or in the .env file "
              "referenced by the _dashboard.env_file section of zf_view.yaml."
        )
    return config


def _s3_client(upload_config: dict[str, Optional[str]]):
    """Create a boto3 S3 client from resolved credentials (lazy boto3 import)."""
    try:
        import boto3
    except ImportError:
        raise SystemExit(
            "ERROR: boto3 is required for the S3 existence check and upload.\n"
            "Install it in the current environment, e.g.:\n"
            "  pip install boto3"
        )
    return boto3.client(
        "s3",
        aws_access_key_id=upload_config["access_key"],
        aws_secret_access_key=upload_config["secret_key"],
        endpoint_url=upload_config["endpoint_url"],
    )


def tiles_exist_on_s3(s3, bucket: str, prefix: str) -> bool:
    """Return True if any object exists under bucket/prefix.

    This is the tile-system presence test: a non-empty prefix means the tile
    pyramid was already built and published, so nothing needs to be redone.
    """
    from botocore.exceptions import ClientError

    normalized_prefix = prefix.strip("/")
    try:
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=normalized_prefix,
            MaxKeys=1,
        )
    except ClientError as exc:
        raise SystemExit(
            f"ERROR: S3 existence check failed on s3://{bucket}/{normalized_prefix} "
            f"({exc}). Check credentials and endpoint."
        )
    return int(response.get("KeyCount", 0)) > 0


def upload_tiles(tiles_dir: Path, s3, bucket: str, prefix: str) -> tuple[int, int]:
    """Upload changed PNG tiles. Returns (uploaded, skipped) counts."""
    from botocore.exceptions import ClientError

    normalized_prefix = prefix.strip("/")
    uploaded = 0
    skipped = 0
    for local_path, rel_key in collect_tile_files(tiles_dir):
        key = f"{normalized_prefix}/{rel_key}" if normalized_prefix else rel_key
        local_size = local_path.stat().st_size
        try:
            head = s3.head_object(Bucket=bucket, Key=key)
            if head.get("ContentLength") == local_size:
                skipped += 1
                continue
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in {"404", "NoSuchKey", "NotFound"}:
                raise
        s3.upload_file(
            str(local_path),
            bucket,
            key,
            ExtraArgs={
                "ContentType": "image/png",
                "ContentDisposition": "inline",
            },
        )
        uploaded += 1
        print(f"uploaded s3://{bucket}/{key}")
    return uploaded, skipped


def main(argv: Optional[list[str]] = None) -> None:
    """Entry point: ensure tiles exist on S3, building and uploading if missing."""
    args = _parse_args(argv)

    views_path, views_src = _resolve_views_path(args.view_path)
    load_environment_from_config(views_path)

    view_name = args.view or os.getenv("HV_DASHBOARD_VIEW") \
        or get_default_endpoint_name(views_path)
    if not view_name:
        raise SystemExit(
            "ERROR: no view selected. Pass --view, set HV_DASHBOARD_VIEW, "
            "or configure _dashboard.default_endpoint in zf_view.yaml."
        )

    view = load_view_config(views_path, view_name)
    tile_build = view.tile_build
    base_dir = views_path.parent.parent

    def _cfg_path(raw: Any) -> Optional[Path]:
        if raw is None:
            return None
        path = Path(str(raw)).expanduser()
        if not path.is_absolute():
            path = base_dir / path
        return path.resolve()

    image_path, image_src = _resolve_param(args.image, _cfg_path(tile_build.source_image), None)
    georef_path, georef_src = _resolve_param(args.georef, _cfg_path(tile_build.georef_file), None)
    vrt_path = _cfg_path(tile_build.vrt_file)
    tif_path = _cfg_path(tile_build.warped_tif)
    rgba_vrt_path = _cfg_path(tile_build.rgba_vrt)
    tiles_dir = _cfg_path(tile_build.tiles_dir)
    if not all([image_path, georef_path, vrt_path, tif_path, rgba_vrt_path, tiles_dir]):
        raise SystemExit(
            f"ERROR: view '{view_name}' is missing required tile_build paths "
            "(source_image, georef_file, vrt_file, warped_tif, rgba_vrt, tiles_dir)."
        )

    min_zoom, min_zoom_src = _resolve_param(args.min_zoom, tile_build.min_zoom, None)
    max_zoom, max_zoom_src = _resolve_param(args.max_zoom, tile_build.max_zoom, None)
    gcp_srs, gcp_srs_src = _resolve_param(args.gcp_srs, tile_build.gcp_srs, None)
    target_srs, target_srs_src = _resolve_param(args.target_srs, tile_build.target_srs, None)
    resampling, resampling_src = _resolve_param(args.resampling, tile_build.resampling, None)
    bucket, bucket_src = _resolve_param(args.bucket, tile_build.s3.bucket, None)
    prefix, prefix_src = _resolve_param(args.prefix, tile_build.s3.prefix, None)

    assert image_path is not None and georef_path is not None
    if not image_path.is_file():
        raise SystemExit(f"ERROR: source image does not exist: {image_path}")
    if not georef_path.is_file():
        raise SystemExit(f"ERROR: georef file does not exist: {georef_path}")
    load_georef_points(georef_path)

    endpoint_url = args.endpoint_url or schema_endpoint_url(views_path, view_name)

    print(f"Views file     : {views_path} ({views_src})")
    print(f"View           : {view_name}")
    print(f"source_image   : {image_path} ({image_src})")
    print(f"georef_file    : {georef_path} ({georef_src})")
    print(f"zoom range     : {min_zoom}-{max_zoom} "
          f"({'flag' if args.min_zoom is not None or args.max_zoom is not None else min_zoom_src})")
    print(f"gcp_srs        : {gcp_srs} ({gcp_srs_src})")
    print(f"target_srs     : {target_srs} ({target_srs_src})")
    print(f"resampling     : {resampling} ({resampling_src})")
    if bucket is not None or prefix is not None:
        s3_desc = f"{bucket or '(unset)'}/{(prefix or '').strip('/')}"
        s3_src = "flag" if args.bucket is not None or args.prefix is not None else bucket_src
        print(f"s3 target      : {s3_desc} ({s3_src})")
    else:
        print("s3 target      : (unset)")
    print(f"endpoint       : {endpoint_url or '(AWS default)'}")
    print()

    # S3 target and credentials are required for a real run; --dry-run
    # stays fully offline.
    offline = args.dry_run
    s3 = None
    normalized_prefix = (prefix or "").strip("/")
    target_desc = f"s3://{bucket or '(unset)'}/{normalized_prefix}"
    if not offline:
        missing_s3 = [
            name for name, val in (("tile_build.s3.bucket", bucket),
                                   ("tile_build.s3.prefix", prefix)) if not val
        ]
        if missing_s3:
            raise SystemExit(
                "ERROR: S3 target not configured: set " + " and ".join(missing_s3)
                + " in zf_view.yaml, or pass --bucket/--prefix."
            )
        upload_config = resolve_upload_config(endpoint_url)
        s3 = _s3_client(upload_config)

    print("[1/6] checking for existing tiles")
    if offline:
        if args.force:
            print("        dry-run + --force: would bypass the existence "
                  "check and plan a full rebuild")
        else:
            print(f"        dry-run: would check whether any object exists under {target_desc}")
        exists = False
    elif args.force:
        print("        --force: bypassing existence check, planning full rebuild")
        exists = False
    else:
        assert bucket is not None
        exists = tiles_exist_on_s3(s3, str(bucket), normalized_prefix)

    if exists:
        print(f"\nTile system already present at {target_desc}; nothing to do.")
        return

    executed = []
    if step_build_gcps_vrt(image_path, georef_path, vrt_path, gcp_srs, args.force, args.dry_run):
        executed.append("gcps_vrt")
    if step_warp(vrt_path, tif_path, target_srs, resampling, args.force, args.dry_run):
        executed.append("warp")
    if step_expand_rgba(tif_path, rgba_vrt_path, args.force, args.dry_run):
        executed.append("rgba")
    if step_generate_tiles(rgba_vrt_path, tiles_dir, int(min_zoom), int(max_zoom),
                           args.force, args.dry_run):
        executed.append("tiles")

    assert tiles_dir is not None

    if offline:
        tile_count = len(collect_tile_files(tiles_dir)) if tiles_dir.is_dir() else 0
        print(f"\n[6/6] dry-run: would upload up to {tile_count} PNG files to {target_desc}")
    else:
        print("\n[6/6] uploading tiles")
        assert bucket is not None
        uploaded, unchanged = upload_tiles(tiles_dir, s3, str(bucket), normalized_prefix)
        print(f"Tiles uploaded: {uploaded}, unchanged/skipped: {unchanged}")

    print(f"\nDone. Steps run: {executed or 'none (all up to date)'}")


if __name__ == "__main__":
    main()
