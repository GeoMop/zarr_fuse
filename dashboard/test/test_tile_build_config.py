from __future__ import annotations

from pathlib import Path

import pytest

from dashboard.config import StoreURI, load_view_config, load_views, parse_s3_uri
from dashboard.scripts.build_overlay_tiles import (
    TARGET_SRS,
    WORK_RGBA_VRT_NAME,
    WORK_TIF_NAME,
    WORK_TILES_DIR_NAME,
    WORK_VRT_NAME,
    resolve_work_dir,
)

VIEW_NAME = "test_view"

# {tile_build_block} is replaced with an indented (4 spaces) tile_build body.
VIEW_TEMPLATE = """\
_dashboard:
  default_view: "test_view"

test_view:
  description: "test view"
  version: "1.0.0"
  reload_interval: 300
  source:
    type: "s3"
    store_type: "zarr"
    uri: "s3://bucket/store.zarr"
    schema_path: "config/test_schema.yaml"
  variable_map:
    fields:
      lat: "latitude"
      lon: "longitude"
      time: "time"
      entity: "station"
  defaults:
    display_variable: "temp"
    group_path: "/"
  visualization:
    map:
      title: "Test"
      point_size: 5
      alpha: 0.5
    timeseries:
      middle_window_days: 30
      right_window_hours: 24
    overlay:
      enabled: false
  tile_build:
{tile_build_block}
"""

VALID_TILE_BUILD = """\
    source_image: "config/source_overlay/12p_final.png"
    georef_file: "config/source_overlay/bukov_georef.json"
    gcp_srs: "EPSG:4326"
    min_zoom: 3
    max_zoom: 18
    warp_resampling: "bilinear"
    tile_resampling: "average"
    target_url: "s3://my-bucket/overlays/my/"
"""

REMOVED_KEYS = ["vrt_file", "warped_tif", "rgba_vrt", "tiles_dir", "target_srs", "s3"]


def _write_view(tmp_path: Path, tile_build_block: str | None) -> Path:
    """Write a minimal but valid zf_view.yaml (+ schema) into tmp_path."""
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "test_schema.yaml").write_text("{}\n", encoding="utf-8")

    if tile_build_block is None:
        text = VIEW_TEMPLATE.replace("  tile_build:\n{tile_build_block}\n", "")
    else:
        text = VIEW_TEMPLATE.replace("{tile_build_block}", tile_build_block)

    views_path = config_dir / "zf_view.yaml"
    views_path.write_text(text, encoding="utf-8")
    return views_path


def test_new_tile_build_shape_loads(tmp_path: Path) -> None:
    """All documented tile_build keys load into the typed config, paths as Path, target as StoreURI."""
    views_path = _write_view(tmp_path, VALID_TILE_BUILD)
    cfg = load_view_config(views_path, VIEW_NAME).tile_build

    assert cfg.source_image == Path("config/source_overlay/12p_final.png")
    assert cfg.georef_file == Path("config/source_overlay/bukov_georef.json")
    assert cfg.gcp_srs == "EPSG:4326"
    assert cfg.min_zoom == 3
    assert cfg.max_zoom == 18
    assert cfg.warp_resampling == "bilinear"
    assert cfg.tile_resampling == "average"
    assert cfg.target_url == StoreURI("s3://my-bucket/overlays/my/")


def test_tile_build_defaults(tmp_path: Path) -> None:
    """An absent tile_build section falls back to the documented defaults."""
    views_path = _write_view(tmp_path, None)
    cfg = load_view_config(views_path, VIEW_NAME).tile_build

    assert cfg.source_image is None
    assert cfg.georef_file is None
    assert cfg.gcp_srs == "EPSG:4326"
    assert cfg.min_zoom == 0
    assert cfg.max_zoom == 20
    assert cfg.warp_resampling == "near"
    assert cfg.tile_resampling is None
    assert cfg.target_url is None


@pytest.mark.parametrize("key", REMOVED_KEYS)
def test_removed_keys_rejected(tmp_path: Path, key: str) -> None:
    block = f'    source_image: "img.png"\n    {key}: "value"\n'
    views_path = _write_view(tmp_path, block)

    with pytest.raises(ValueError, match=key):
        load_views(views_path)


def test_old_resampling_key_rejected(tmp_path: Path) -> None:
    block = '    source_image: "img.png"\n    resampling: "bilinear"\n'
    views_path = _write_view(tmp_path, block)

    with pytest.raises(ValueError, match="resampling"):
        load_views(views_path)


def test_multiple_unknown_keys_listed(tmp_path: Path) -> None:
    block = (
        '    source_image: "img.png"\n'
        '    resampling: "bilinear"\n'
        '    tiles_dir: "tiles"\n'
    )
    views_path = _write_view(tmp_path, block)

    with pytest.raises(ValueError) as exc_info:
        load_views(views_path)

    message = str(exc_info.value)
    assert "resampling" in message
    assert "tiles_dir" in message
    assert "Allowed keys:" in message


def test_invalid_target_url_rejected(tmp_path: Path) -> None:
    """A non-s3 target_url fails early at config load instead of at build time."""
    block = '    target_url: "https://example.com/tiles/"\n'
    views_path = _write_view(tmp_path, block)

    with pytest.raises(ValueError, match="target_url"):
        load_views(views_path)


def test_parse_s3_uri() -> None:
    """The shared parser splits bucket/prefix and rejects non-s3 URIs."""
    assert parse_s3_uri("s3://bukov/overlays/bukov/") == ("bukov", "overlays/bukov")
    assert parse_s3_uri("s3://bukov") == ("bukov", "")
    assert parse_s3_uri("  s3://bukov/a  ") == ("bukov", "a")
    assert parse_s3_uri("https://bukov/a") is None
    assert parse_s3_uri("s3:///no-bucket") is None
    assert parse_s3_uri("bukov/a") is None


def test_internal_work_paths_derived(tmp_path: Path) -> None:
    work_dir = resolve_work_dir(tmp_path, "my_view")
    assert work_dir == (tmp_path / "workdir" / "tile_build" / "my_view").resolve()

    assert WORK_VRT_NAME == "source_gcps.vrt"
    assert WORK_TIF_NAME == "source_3857.tif"
    assert WORK_RGBA_VRT_NAME == "source_3857_rgba.vrt"
    assert WORK_TILES_DIR_NAME == "tiles"


def test_target_srs_fixed() -> None:
    assert TARGET_SRS == "EPSG:3857"
