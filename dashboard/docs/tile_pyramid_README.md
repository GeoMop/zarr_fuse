# High-Resolution Overlay via Tile Pyramid

## Overview

This project uses a **tile pyramid** approach to display large georeferenced overlay images on top of a GeoViews/Bokeh map.

The main reason for this approach is performance.

A very large overlay image can be displayed correctly on the map, but loading and rendering the whole raster directly in the browser is expensive. It causes:

- large initial downloads
- heavy browser-side rendering
- poor scalability at deeper zoom levels
- unnecessary runtime work for the dashboard server

To avoid this, the overlay is preprocessed into **small PNG tiles** at multiple zoom levels. At runtime, the dashboard only loads the tiles needed for the current map extent and zoom level.

---

## Why this approach was chosen

Several approaches were evaluated.

### Native overlay / `regrid` / `rasterize`
These approaches can be useful when only a visible portion of the overlay needs to be dynamically processed or displayed.

They are not a good fit here because this project needs to display the **full overlay**, not just a small dynamically computed subset. That means the main bottleneck remains the large initial transfer and rendering cost.

Also, these approaches do not provide true multi-resolution tile behavior in the same way that map tile systems do.

### Viewport crop
This approach renders only the visible area at runtime.

It reduces memory pressure, but it introduces heavy server-side work during every pan or zoom event. In testing, this caused noticeable latency and a poor interactive experience.

### Tile pyramid
This approach preprocesses the overlay once and produces static PNG tiles across multiple zoom levels.

This has several advantages:

- the overlay remains correctly georeferenced
- the browser loads only visible tiles
- the initial view is lighter
- zooming can request more detailed tiles
- no heavy raster processing is needed during interaction

This is the approach currently used in the project.

---

## How the tile pyramid works

The preprocessing pipeline converts a georeferenced source image into web map tiles.

The general workflow is:

1. read the source image
2. read georeference control points
3. attach GCPs to the image
4. warp the image into **EPSG:3857 / Web Mercator**
5. expand to RGBA if needed
6. generate XYZ tile folders (`z/x/y.png`)
7. serve the tiles as static files

At runtime, GeoViews reads the tile URL pattern and overlays the tiles on top of the base map.

---

## Important concepts

### RGBA
RGBA describes the **pixel color values** of the image:

- R = red
- G = green
- B = blue
- A = alpha / transparency

### EPSG:3857 / Web Mercator
Web Mercator is the coordinate system used by typical web tile maps such as OSM-based maps in Bokeh/GeoViews.

The overlay must be reprojected into this CRS so that it aligns correctly with the base map.

### XYZ tiles
The generated tiles follow the standard web map pattern:

`{Z}/{X}/{Y}.png`

Where:

- `Z` = zoom level
- `X` = tile column
- `Y` = tile row

---

## Project-specific workflow

### Source files

For each overlay endpoint, the project expects:

- a source image (for example `.png`)
- a georeference file containing control points

Examples in this project:

#### Bukov
- `dashboard/config/bukov_endpoint/12p_final.png`
- `dashboard/config/bukov_endpoint/bukov_georef.json`

#### Surface data
- `dashboard/config/surface_data_endpoint/mapa_uhelna_vyrez.png`
- `dashboard/config/surface_data_endpoint/uhelna_georef.geo.json`

---

## Preprocessing environment

The preprocessing is done outside the dashboard runtime using a dedicated **Miniconda + GDAL** environment.

Example environment setup:

```powershell
conda create --yes --name gdal-test python=3.11
conda activate gdal-test
conda install --yes -c conda-forge gdal
```

This environment is used only for raster preprocessing and tile generation.

The dashboard itself does not need GDAL at runtime if it only consumes already generated tiles.

---

## Preprocessing steps used in this project

### 1. Create a GCP-based VRT
A script reads the georeference file and attaches control points to the source image.

Example:

```powershell
python .\dashboard\scripts\prepare_bukov_gcps.py
```

This creates a file such as:

- `bukov_gcps.vrt`

### 2. Warp to Web Mercator
The GCP VRT is warped into EPSG:3857.

Example:

```powershell
gdalwarp -t_srs EPSG:3857 -r near -dstalpha -overwrite -of GTiff -co TILED=YES -co COMPRESS=DEFLATE .\dashboard\config\bukov_endpoint\bukov_gcps.vrt .\dashboard\config\bukov_endpoint\bukov_3857.tif
```

This creates a file such as:

- `bukov_3857.tif`

### 3. Expand to RGBA if needed
If the raster is palette-based, it is expanded to RGBA before tiling.

Example:

```powershell
gdal_translate -of VRT -expand rgba .\dashboard\config\bukov_endpoint\bukov_3857.tif .\dashboard\config\bukov_endpoint\bukov_3857_rgba.vrt
```

This creates:

- `bukov_3857_rgba.vrt`

### 4. Generate XYZ tiles
Tiles are generated across a chosen zoom range.

Example:

```powershell
python -m osgeo_utils.gdal2tiles --xyz -z 0-20 .\dashboard\config\bukov_endpoint\bukov_3857_rgba.vrt .\dashboard\config\bukov_endpoint\tiles
```

This creates:

- `dashboard/config/bukov_endpoint/tiles/`

with folders like:

- `0/`
- `1/`
- `2/`
- ...
- `20/`

and PNG files inside the nested `x/y.png` structure.

---

## How tiles are served in development

For local testing, the tile folder can be served with a simple Python static server.

Example for Bukov:

```powershell
cd C:\Users\fatih\Documents\GitHub\zarr_fuse\dashboard\config\bukov_endpoint\tiles
python -m http.server 8000
```

Then tiles are available under:

`http://localhost:8000/{Z}/{X}/{Y}.png`

---

## How the dashboard uses the tiles

The dashboard reads a tile URL from an environment variable.

Example in PowerShell:

```powershell
$env:HV_OVERLAY_TILE_URL = "http://localhost:8000/{Z}/{X}/{Y}.png"
```

In Python, the overlay loader reads that value:

```python
tile_url = os.getenv("HV_OVERLAY_TILE_URL", "").strip()
```

If the variable is present, the overlay is added as a tile layer:

```python
gv.WMTS(tile_url)
```

This means:

- the app itself does not load the original large PNG
- the app does not compute georeferencing at runtime
- the browser simply requests the tile images it needs

---

## Why the overlay may disappear when zooming

If the overlay disappears at deeper zoom levels, the most common reason is that the tile pyramid was not generated to a high enough zoom.

Example:

- if tiles were only generated with `-z 0-12`
- but the browser requests zoom 14 or 15
- the tile server returns `404`
- the overlay disappears at those levels

Fix:
generate more zoom levels, for example:

```powershell
python -m osgeo_utils.gdal2tiles --xyz -z 0-20 ...
```

---

## Why zooming does not always improve quality

A tile pyramid improves delivery and interaction, but it does **not create new detail**.

If the original overlay image is already soft, compressed, or weakly georeferenced, deeper zoom levels may still look blurry.

The main quality limits are usually:

- source image quality
- number and quality of georeference control points
- resampling during warp

---

## Generated helper files

`gdal2tiles` may also generate files such as:

- `googlemaps.html`
- `leaflet.html`
- `openlayers.html`
- `mapml.mapml`

These are viewer/demo helper files generated automatically by GDAL.

They are **not used by the dashboard** and can be ignored.

---

## Runtime vs preprocessing responsibilities

### Preprocessing stage
Handled outside the dashboard:

- GCP attachment
- reprojection
- RGBA expansion
- tile generation

### Runtime stage
Handled inside the dashboard:

- read tile URL
- load tile layer with GeoViews
- display it on top of the base map

This separation is intentional and keeps the dashboard responsive.

---

## Deployment plan

The recommended deployment pattern is:

1. keep source image, georef file, and preprocessing scripts in the repo
2. generate tiles as a build/deployment step
3. publish tiles to a static location on the server or object storage
4. configure the dashboard to use the deployed tile URL

Example production tile URL:

`https://your-server/static/bukov/{Z}/{X}/{Y}.png`

Generated tiles should generally be treated as **build artifacts**, not source files.

---

## Recommended repository policy

Keep in version control:

- source images
- georeference files
- preprocessing scripts
- endpoint-specific configuration

Usually ignore generated outputs such as:

- `tiles/`
- `*_gcps.vrt`
- `*_3857.tif`
- `*_3857_rgba.vrt`

Example `.gitignore` entries:

```gitignore
dashboard/config/**/tiles/
dashboard/config/**/*_gcps.vrt
dashboard/config/**/*_3857.tif
dashboard/config/**/*_3857_rgba.vrt
```

---

## Reusable workflow

This approach can be generalized for multiple overlays.

A reusable version should accept parameters such as:

- source image path
- georef file path
- output directory
- min/max zoom
- resampling mode

That would allow the same preprocessing pipeline to be reused for:

- `bukov_endpoint`
- `surface_data_endpoint`
- future overlays

---

## Summary

The tile pyramid approach is used in this project because it is the most practical solution for large georeferenced overlays that must remain interactive inside a GeoViews/Bokeh dashboard.

It moves heavy raster work out of the live app path and replaces a single large raster overlay with static multi-resolution PNG tiles.

This gives:

- better browser behavior
- fluid panning and zooming
- cleaner deployment
- a reusable preprocessing workflow for additional overlays
