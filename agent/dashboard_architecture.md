
-------------------------------------------------------------------------------Planned Headers----------------------------------------------------------------------------------
## 1. Scope

## 2. Architecture Overview

## 3. Runtime Components

## 4. Session Lifecycle

## 5. State Ownership

## 6. Component Communication

### 6.1 `composed.py` ↔ `config.py`

### 6.2 `composed.py` ↔ `data.py`

### 6.3 `composed.py` ↔ `sidebar.py`

### 6.4 `composed.py` ↔ `map_views.py`

### 6.5 `composed.py` ↔ `plot_selection.py`

### 6.6 `composed.py` ↔ `multi_time_views.py`

## 7. Important Variables and Data Passed Between Components

### 7.1 Configuration variables

### 7.2 Dashboard state

### 7.3 Map state

### 7.4 Selection state

### 7.5 Timeseries data

### 7.6 Callback arguments and return values

## 8. User Interaction Flows

### 8.1 Dashboard startup

### 8.2 Endpoint change

### 8.3 Group change

### 8.4 Variable change

### 8.5 Map click

### 8.6 Selection-table change

### 8.7 Timeseries interaction

## 9. Configuration and Environment Variables

## 10. Data Contracts

### 10.1 Map data contract

### 10.2 Timeseries data contract

### 10.3 Selected-site contract

### 10.4 Error result contract

## 11. Caching and State Lifetime

## 12. Error Handling

## 13. Architectural Invariants

## 14. Risks and Tight Coupling

## 15. Verification
-------------------------------------------------------------------------------Previous section will be removed in the feature------------------------------------------------------


<!-- Meta
scope: dashboard/ — HoloViz Panel dashboard for Zarr Fuse endpoints
readers: developers, maintainers, AI coding agents
branch: feature/dashboard-multiple-selection
commit_sha: 2e7b2d92dd7f5b031ee66d44239d34910a81447b
last_reviewed: 03 08 2026
verification:
  - static code inspection of the branch above
  - tests: not run during this documentation pass
  - runtime verification: not performed
-->

# dashboard/ Architecture

## 1. Scope

This document describes the runtime architecture of `dashboard/`, with emphasis on:

- which component creates and owns important state,
- which values are passed between modules,
- the shape of the main data contracts,
- which callbacks mutate state,
- and which state changes trigger table or plot refreshes.

For file navigation and change-impact guidance, see
[dashboard_map.md](dashboard_map.md).

This is intentionally a starting point. Sections below contain only behavior
verified directly from the current branch.

---

## 2. Runtime Entry and Composition

The installed command is:

```text
zf-dashboard
  → dashboard.serve_dashboard:main
```

`serve_dashboard.py` loads dashboard environment configuration before importing
the main dashboard composition module. `main()` then calls `pn.serve` with
`build_dashboard` registered for both `/` and `/app`.

High-level composition:

```text
serve_dashboard.py
    |
    | calls build_dashboard
    v
composed.py
    |
    +--> config.py
    |      endpoints path + endpoint configuration
    |
    +--> data.py
    |      DashboardData + LocalClient
    |
    +--> sidebar.py
    |      controller widgets
    |
    +--> plot_selection.py
    |      SelectionState + Tabulator panel
    |
    +--> map_views.py
    |      map_view + map_state
    |
    +--> multi_time_views.py
           year/month/day DynamicMaps + on_map_tap callback
```

`build_dashboard()` is the composition root: it creates the objects used by the
dashboard, wires callbacks between them, places the views into a GoldenLayout
template, and returns the resulting `pn.Template`.

---

## 3. Startup Sequence

The verified startup sequence inside `build_dashboard()` is:

```text
resolve_endpoints_path()
    ↓
get_default_endpoint_name(endpoints_path)
    ↓
load_endpoints(endpoints_path)
    ↓
choose endpoint_name
    ↓
load_data(
    "local",
    endpoint_name=endpoint_name,
    endpoints_path=endpoints_path,
    display_variable=""
)
    ↓
DashboardData + LocalClient
    ↓
LocalClient.get_endpoints()
LocalClient.get_endpoint()
LocalClient.get_structure()
    ↓
build_sidebar(...)
    ↓
data.group_path = node_select.value
    ↓
resolve_available_dimensions(...)
    ↓
build_plot_selection_panel(...)
    ↓
panel_table + SelectionState
    ↓
create tap_stream
    ↓
build_map_view(data, tap_stream)
    ↓
map_view + map_state
    ↓
build_timeseries_views(
    data,
    map_state,
    selection_state,
    render_spinner
)
    ↓
line_left + line_mid + line_right + on_map_tap
    ↓
wire widget/stream callbacks
    ↓
assemble and return pn.Template
```

---

## 4. Main State Owners

### 4.1 `DashboardData`

Defined in `data.py`:

```python
@dataclass
class DashboardData:
    endpoint_name: str
    group_path: str
    display_variable: str
    client: LocalClient
```

`load_data()` constructs this object. `composed.py` keeps the object in its
`data` variable and passes it to both `build_map_view()` and
`build_timeseries_views()`.

The fields are mutated by dashboard callbacks:

| Field | Meaning in current code | Mutated by |
|---|---|---|
| `endpoint_name` | selected endpoint name | endpoint switch |
| `group_path` | selected group/node path | startup, endpoint switch, node change |
| `display_variable` | selected data variable | variable selection, endpoint switch |
| `client` | `LocalClient` used for configuration and Zarr reads | created by `load_data()` |

### 4.2 `LocalClient`

`DashboardData.client` is a `LocalClient`.

It stores:

```text
endpoints_path
base_dir
_nodes
_map_data_cache
_timeseries_cache
```

It is the data-access boundary used by the dashboard to:

```text
load endpoint configuration
open Zarr nodes
read group structure
list variables
read variable metadata
read map data
read timeseries data
```

`clear_cache()` clears `_map_data_cache` and `_timeseries_cache`. It does not
clear `_nodes`.

### 4.3 `SelectionState`

`build_plot_selection_panel()` creates a `SelectionState` when no existing state
is passed.

Public Param fields:

```text
version
layout_version
row_dim
col_dim
```

Important internal state:

```text
_sites
_checked
_row_shapes
_col_colors
_valid_count
_checked_count
```

The canonical checked-selection key is:

```text
(site_id: str, depth_value: float)
```

This key does not depend on whether the table currently displays entities as
rows or columns.

### 4.4 `map_state`

`build_map_view()` returns:

```text
(map_view, map_state)
```

The returned `map_state` has these keys in both the normal and empty-data paths:

```python
{
    "lats": np.ndarray,
    "lons": np.ndarray,
    "marker_meta": list,
    "variable": str,
    "data_error_reason": str | None,
}
```

`composed.py` passes this object to `build_timeseries_views()`.

### 4.5 `tap_stream`

`composed.py` creates:

```python
tap_stream = streams.Tap(x=None, y=None)
```

The same object is passed to `build_map_view()`. `map_views.py` assigns the map
DynamicMap as `tap_stream.source`.

`composed.py` watches changes to `tap_stream.x` and `tap_stream.y` and forwards
captured coordinates to the current `on_map_tap` callback.

---

## 5. Component Communication Contracts

### 5.1 `build_sidebar(...)`

Inputs from `composed.py` include:

```text
endpoint_name
endpoint_config
structure
endpoints
loading_indicator
timeseries_loading
render_spinner
rendering_status
table_loading
```

It returns exactly seven objects:

```text
controller
store_selector
tree_view
variable_selector
variable_metadata
node_hint
store_info
```

`composed.py` receives `tree_view` under the local name `node_select`.

The status widgets passed into `build_sidebar()` are also observed by sidebar
callbacks so the sidebar status display reflects their `visible` state.

### 5.2 `build_plot_selection_panel(...)`

Important inputs:

```text
state
available_dims
plot_var_selector
table_loading
```

Return value:

```text
(panel, state)
```

The returned state is the `SelectionState` later passed into
`build_timeseries_views()`.

### 5.3 `build_map_view(data, tap_stream)`

Inputs:

```text
data        DashboardData-like object used for endpoint/group/variable access
tap_stream  HoloViews Tap stream
```

Return value:

```text
(map_view, map_state)
```

The function also assigns a DataFrame to:

```text
data.current_map_df
```

`current_map_df` is not declared as a field of the `DashboardData` dataclass; it
is attached by `map_views.py` and read by the map DynamicMap callback.

### 5.4 `build_timeseries_views(...)`

Current signature:

```python
build_timeseries_views(
    data,
    map_state,
    selection_state,
    render_spinner=None,
)
```

Return value:

```text
line_left
line_mid
line_right
on_map_tap
```

The three line objects are HoloViews `DynamicMap` objects.

`composed.py` stores the callback in:

```python
map_handlers["on_map_tap"]
```

When views are rebuilt, that dictionary entry is replaced with the new callback.
This lets the existing tap watcher call the callback belonging to the latest
timeseries view construction.

---

## 6. Data Contracts Passed Between Components

### 6.1 Map data: `LocalClient.get_map_data()`

Relevant inputs:

```text
endpoint_name
group_path
variable
time_index
depth_index
```

Successful result:

```python
{
    "status": "success",
    "lat": list[float | None],
    "lon": list[float | None],
    "values": list[float | None],
    "entities": list | None,
    "marker_meta": list[dict],
    "has_value": list[bool],
    "variable": str,
    "time_index": int,
    "depth_index": int,
}
```

Each `marker_meta` item is constructed with:

```python
{
    "entity_index": int,
    "site_id": str | None,
    "value": float | None,
    "has_value": bool,
}
```

Error results use:

```python
{
    "status": "error",
    "reason": str,
}
```

`build_map_view()` converts the map payload into NumPy arrays and into the
smaller `map_state` object passed to the timeseries component.

### 6.2 Timeseries data: `LocalClient.get_timeseries_data()`

Relevant inputs:

```text
endpoint_name
group_path
lat
lon
variable
entity_index
```

Successful result:

```python
{
    "status": "success",
    "times": list[str],
    "depths": list[float | None],
    "series": list[list[float | None]],
    "variable": str,
    "borehole_index": int,
    "borehole_name": str | None,
}
```

Error results use:

```python
{
    "status": "error",
    "reason": str,
}
```

Inside `multi_time_views.py`, successful times are converted with
`pd.to_datetime`, depths are converted to a NumPy float array, and each series
is converted to a NumPy float array before the values are passed into
`SelectionState.add_site()`.

### 6.3 Registered site inside `SelectionState`

`SelectionState.add_site()` stores each registered site as:

```python
{
    "entity_index": int,
    "site_id": str,
    "depths": np.ndarray,
    "series": list,
    "times": object,
}
```

In the current timeseries path, `times` is a pandas datetime-like object produced
by `pd.to_datetime`, and the series elements are NumPy float arrays.

### 6.4 Selected combinations passed to plotting

`SelectionState.get_selected_combinations()` returns:

```text
list[(entity_index, depth_idx)]
```

`multi_time_views.py` uses those pairs to find:

```text
site = site_lookup[entity_index]
series = site["series"][depth_idx]
depth = site["depths"][depth_idx]
```

This is the bridge between the canonical `(site_id, depth_value)` selection
stored by `SelectionState` and the numeric indices used to build curves.

---

## 7. State Change and Communication Flows

### 7.1 Endpoint change

`store_selector.value` is watched by `on_store_change()`.

Verified flow:

```text
new store_selector value
    ↓
_switch_endpoint(selected_endpoint)
    ↓
endpoint_name = selected_endpoint
data.endpoint_name = selected_endpoint
data.group_path = "/"
data.display_variable = ""
    ↓
data.client.clear_cache()
selection_state.clear()
    ↓
_refresh_sidebar_for_endpoint(selected_endpoint)
    ↓
reload endpoint list/config/structure
replace node_select options/value
repopulate variable selector
    ↓
refresh_views()
```

### 7.2 Group/node change

`node_select.value` is watched by `on_node_change()`.

Verified flow:

```text
event.new
    ↓
data.group_path = event.new
selection_state.clear()
    ↓
_populate_variable_selector(...)
    ↓
refresh_views()
```

### 7.3 Variable change

`variable_selector.value` is watched by `on_variable_change()`.

The display label is converted back to the variable name and passed to
`_select_variable()`.

Verified state mutation:

```text
data.display_variable = var_name
```

Then `refresh_views()` rebuilds the map and timeseries views.

Before rebuilding, `refresh_views()` saves the currently registered
`entity_index` values. After rebuilding, it uses the new `map_state` latitude
and longitude arrays and calls the new `on_map_tap(lon, lat)` callback for those
saved indices. In the current timeseries implementation, an existing site with
the same `entity_index` is updated through `SelectionState.add_site(...,
force=True)`.

### 7.4 Map tap → selected site

Communication path:

```text
map interaction
    ↓
tap_stream.x / tap_stream.y
    ↓
composed.on_tap_event()
    ↓
map_handlers["on_map_tap"](x, y)
    ↓
multi_time_views.on_map_tap(x, y)
    ↓
nearest marker selected from:
    map_state["lats"]
    map_state["lons"]
    map_state["marker_meta"]
    ↓
_fetch_timeseries(lat, lon, marker_meta)
    ↓
LocalClient.get_timeseries_data(
    endpoint_name=data.endpoint_name,
    group_path=data.group_path,
    variable=data.display_variable,
    entity_index=marker_meta["entity_index"],
    ...
)
    ↓
SelectionState.add_site(...)
```

For a real map click, the current code requires the nearest marker to be within
a `0.0002` degree threshold before loading the timeseries.

### 7.5 Table edit → plot redraw

Browser-side Tabulator edits are synchronized into `_on_edit_cell()`.

Verified flow:

```text
table edit
    ↓
SelectionState.set_checked(..., bump_version=False)
    ↓
_schedule_bump()
    ↓
150 ms default trailing delay
    ↓
state.version += 1
    ↓
three timeseries DynamicMaps observe SelectionState.version
    ↓
timeseries views redraw
```

`layout_version` is separate. The plot-selection panel watches
`layout_version` and calls `_rebuild_table()` when it changes.

---

## 8. Timeseries Linking

`build_timeseries_views()` creates three DynamicMaps:

```text
line_left   full time extent
line_mid    middle configured window
line_right  right configured window
```

The function creates a shared `center_stream`.

Taps on any of the three timeseries views are watched by
`update_center_from_tap()`, which converts the tapped x value to a pandas
datetime and emits it through `center_stream`.

All three DynamicMaps include `center_stream` in their stream list. Their
rendered overlays include a red `VLine` at the current center time.

The middle and right spans come from endpoint visualization configuration:

```text
visualization.timeseries.middle_window_days
visualization.timeseries.right_window_hours
```

---

## 9. Configuration Flow

### 9.1 Endpoint config path

`config.py` defines:

```text
ENDPOINTS_PATH
SCHEMAS_PATH
```

`resolve_endpoints_path()` resolves `endpoints.yaml` in this order:

```text
1. ENDPOINTS_PATH environment variable
2. upward search from current working directory:
   dashboard/config/endpoints.yaml
   config/endpoints.yaml
   app/databuk/config/endpoints.yaml
```

If `ENDPOINTS_PATH` is set but does not exist, the function raises
`FileNotFoundError`.

### 9.2 Schema directory

While building an endpoint configuration:

```text
if SCHEMAS_PATH is set:
    it must be a directory
    schema path = SCHEMAS_PATH / basename(configured schema_path)
else:
    use the configured schema path
    resolve relative paths against base_dir
```

### 9.3 `.env` loading

`load_environment_from_config()` accepts either:

```text
env_file
```

or:

```text
_dashboard.env_file
```

from `endpoints.yaml`.

It calls:

```python
load_dotenv(env_path, override=False)
```

so an already-present environment variable is not replaced by the `.env` file.

### 9.4 Environment substitution inside endpoint data

Before an endpoint is converted to `EndpointConfig`,
`_process_environment_variables()` recursively replaces `${VAR}` occurrences in
strings using `os.getenv(VAR)`.

A referenced environment variable that does not exist raises `ValueError`.

---

## 10. Deployment → Runtime Configuration Boundary

The reusable dashboard deployment workflow creates a ConfigMap from:

```text
source endpoints.yaml
source schemas directory
```

The Helm call mounts them into the dashboard container at:

```text
/opt/dashboard/config/endpoints.yaml
/opt/dashboard/schemas
```

and injects:

```text
ENDPOINTS_PATH=/opt/dashboard/config/endpoints.yaml
SCHEMAS_PATH=/opt/dashboard/schemas
```

The Helm frontend deployment template renders the supplied `extraEnv`,
`extraVolumes`, and `extraVolumeMounts` into the container.

The frontend secret contains:

```text
ZF_S3_ACCESS_KEY
ZF_S3_SECRET_KEY
ZF_S3_ENDPOINT_URL
```

and the deployment imports that Secret with `envFrom`.

`serve_dashboard.py` performs environment bootstrap before importing
`dashboard.composed` and `dashboard.tile_service`.

---

## 11. Cache Ownership

### 11.1 `LocalClient`

Per `LocalClient` instance:

```text
_nodes
_map_data_cache
_timeseries_cache
```

Map cache key:

```text
endpoint:group:variable:time_index:depth_index
```

Timeseries cache key:

```text
endpoint:group:variable:lat(8dp):lon(8dp):entity_index
```

`clear_cache()` clears map and timeseries caches only.

### 11.2 Tile service

`tile_service.py` has a module-level:

```text
cache: dict[str, dict]
```

It is initialized from `tile_url_cache.json` and updated when a new presigned
tile URL is generated.

---

## 12. Verified Coupling Points

These relationships are important when changing architecture:

1. **`composed.py` is the central coordinator.** It imports and wires config,
   data, sidebar, plot selection, map, and timeseries components.

2. **`multi_time_views.py` imports a private config helper.**
   It directly imports:

   ```python
   dashboard.config._resolve_fields_for_group_raw
   ```

3. **`map_views.py` adds `current_map_df` dynamically to `DashboardData`.**
   That attribute is later read by the map DynamicMap callback.

4. **`SelectionState.version` is a plotting signal.**
   The three timeseries DynamicMaps subscribe to it through
   `streams.Params(selection_state, parameters=["version"])`.

5. **`SelectionState.layout_version` is a table-layout signal.**
   The plot-selection panel watches it and rebuilds the Tabulator table.

6. **Marker identity crosses the map/timeseries boundary through
   `marker_meta["entity_index"]`.**
   The selected marker's entity index is passed to
   `LocalClient.get_timeseries_data()`.

---

## 13. Not Yet Documented

The following areas should be added only after separate verification:

- exact Panel session/concurrency behavior,
- lifecycle/cleanup behavior when a client disconnects,
- runtime behavior under multiple simultaneous users,
- performance characteristics under large datasets,
- production failure/retry behavior,
- full overlay/tile-service architecture,
- complete deployment lifecycle.

Do not infer these from static structure alone.

---

## 14. Source Files Used for This Starting Point

Static inspection was limited to the current branch versions of:

```text
dashboard/pyproject.toml
dashboard/serve_dashboard.py
dashboard/composed.py
dashboard/config.py
dashboard/data.py
dashboard/sidebar.py
dashboard/map_views.py
dashboard/multi_time_views.py
dashboard/plot_selection.py
dashboard/tile_service.py
dashboard/charts/holoviz/templates/frontend/deployment.yaml
dashboard/charts/holoviz/templates/frontend/secret.yaml
.github/workflows/dashboard-reusable-workflow.yaml
```


