"""Plot selection panel — table-based site/depth selector."""

from __future__ import annotations

import numpy as np
import panel as pn
import param


class SelectionState(param.Parameterized):
    """Canonical selection state for the plot selection table.

    Drives both the visible table (via *layout_version*) and the timeseries
    plots (via *version*).  The two version counters let callers distinguish
    between "rebuild the widget tree" and "just redraw plots".

    The canonical selection store is ``_checked: set[(site_id, depth)]``,
    always keyed by site_id and depth_value regardless of the current
    table row/column orientation.
    """

    version = param.Integer(default=0)
    layout_version = param.Integer(default=0)
    row_dim = param.String(default="entity")
    col_dim = param.String(default="vertical")

    def __init__(self, entity_field="entity", vertical_field="vertical", **params):
        super().__init__(**params)
        self.entity_field = entity_field
        self.vertical_field = vertical_field
        self._sites: list[dict] = []
        self._checked: set[tuple[str, float]] = set()
        self._row_shapes: dict[str, str] = {}
        self._col_colors: dict[str, str] = {}

    # ── read-only view of internal data ──────────────────────────────

    @property
    def sites(self) -> list[dict]:
        """Snapshot of currently registered sites."""
        return list(self._sites)

    # ── row / column key helpers ─────────────────────────────────────

    @property
    def row_keys(self) -> list:
        """Labels for current table rows, determined by *row_dim*."""
        if self.row_dim == "entity":
            return [s["site_id"] for s in self._sites]
        return self.all_depths

    @property
    def col_keys(self) -> list:
        """Labels for current table columns, determined by *col_dim*."""
        if self.col_dim == "vertical":
            return self.all_depths
        return [s["site_id"] for s in self._sites]

    def _resolve_canonical(self, row_key, col_key) -> tuple[str, float]:
        """Convert *(row_key, col_key)* to canonical *(site_id, depth)*.

        Works correctly in both orientations so the caller can always
        store / query ``_checked`` with the same key type.
        """
        if self.row_dim == "entity":
            return str(row_key), float(col_key)
        return str(col_key), float(row_key)

    def _site_has_depth(self, site_id: str, depth_value: float) -> bool:
        """Return ``True`` if *depth_value* exists for the given site."""
        for site in self._sites:
            if site["site_id"] == site_id:
                return float(depth_value) in set(
                    float(d) for d in np.asarray(site["depths"]).ravel()
                )
        return False

    def is_valid(self, row_key, col_key) -> bool:
        """Return ``True`` if *(row_key, col_key)* is a valid combination."""
        site_id, depth_value = self._resolve_canonical(row_key, col_key)
        return self._site_has_depth(site_id, depth_value)

    def is_checked(self, row_key, col_key) -> bool | None:
        """Return checked state for *(row_key, col_key)*.

        Returns ``None`` for invalid combinations.
        """
        if not self.is_valid(row_key, col_key):
            return None
        site_id, depth_value = self._resolve_canonical(row_key, col_key)
        return (str(site_id), float(depth_value)) in self._checked

    def set_checked(self, row_key, col_key, value: bool):
        """Mark *(row_key, col_key)* as checked or unchecked.

        Only affects valid combinations; invalid combos are silently
        ignored.  Bumps *version* on change.
        """
        if not self.is_valid(row_key, col_key):
            return
        site_id, depth_value = self._resolve_canonical(row_key, col_key)
        key = (str(site_id), float(depth_value))
        before = key in self._checked
        if value and not before:
            self._checked.add(key)
        elif not value and before:
            self._checked.discard(key)
        else:
            return  # no change
        self.version += 1

    # ── mutations ────────────────────────────────────────────────────

    def add_site(self, entity_index, site_id, depths, series, times, force=False):
        """Register a new site (or replace data if *force* is True and site exists).

        When *force* is True and the entity_index already exists, the series/times
        data is replaced (e.g. after a variable change).  Depth auto-selection is
        **not** re-applied — existing checked states are preserved.
        Triggers a table rebuild (**layout_version*).
        """
        for site in self._sites:
            if site["entity_index"] == entity_index:
                if not force:
                    print(f"[SelectionState] Site {site_id} (idx={entity_index}) already added, skipping")
                    return
                # Replace data for a re-fetch (e.g. after variable change)
                site["depths"] = np.asarray(depths, dtype=float).ravel()
                site["series"] = series
                site["times"] = times
                self.layout_version += 1
                self.version += 1
                print(f"[SelectionState] Updated data for site {site_id} (idx={entity_index})")
                return

        depths_arr = np.asarray(depths, dtype=float).ravel()

        self._sites.append({
            "entity_index": int(entity_index),
            "site_id": str(site_id),
            "depths": depths_arr,
            "series": series,
            "times": times,
        })

        for d in depths_arr:
            dv = float(d)
            if not np.isfinite(dv):
                continue  # skip NaN/Inf — NaN != NaN breaks set lookup
            key = (str(site_id), dv)
            self._checked.add(key)

        self.layout_version += 1
        self.version += 1
        print(f"[SelectionState] Added site {site_id} (idx={entity_index}), "
              f"depths={depths_arr.tolist()}")

    def remove_site(self, entity_index):
        """Remove a site by its *entity_index*.  Triggers both counters."""
        site = None
        for s in self._sites:
            if s["entity_index"] == entity_index:
                site = s
                break

        if site is None:
            return

        site_id = site["site_id"]
        self._sites.remove(site)

        for k in list(self._checked):
            if k[0] == site_id:
                self._checked.discard(k)

        self.layout_version += 1
        self.version += 1
        print(f"[SelectionState] Removed site {site_id} (idx={entity_index})")

    def set_selected(self, site_id, depth_value, value):
        """Check/uncheck a single (site, depth) cell.  Triggers plot re-render."""
        key = (str(site_id), float(depth_value))
        if value:
            self._checked.add(key)
        else:
            self._checked.discard(key)
        self.version += 1

    def clear(self):
        """Remove all sites and reset selection."""
        self._sites.clear()
        self._checked.clear()
        self.layout_version += 1
        self.version += 1
        print("[SelectionState] Cleared all sites")

    def select_all(self):
        """Check every (site, depth) cell."""
        changed = False
        for site in self._sites:
            for d in site["depths"]:
                k = (str(site["site_id"]), float(d))
                if k not in self._checked:
                    self._checked.add(k)
                    changed = True
        if changed:
            self.version += 1

    def deselect_all(self):
        """Uncheck every (site, depth) cell."""
        if self._checked:
            self._checked.clear()
            self.version += 1

    def set_all_for_row(self, row_key, value: bool):
        """Check or uncheck all valid combos in the given row."""
        changed = False
        for site in self._sites:
            depths_arr = np.asarray(site["depths"]).ravel()
            for depth_val in depths_arr:
                if self.row_dim == "entity":
                    row_matches = (str(site["site_id"]) == str(row_key))
                else:
                    row_matches = (float(depth_val) == float(row_key))
                if not row_matches:
                    continue
                key = (str(site["site_id"]), float(depth_val))
                before = key in self._checked
                if value and not before:
                    self._checked.add(key)
                    changed = True
                elif not value and before:
                    self._checked.discard(key)
                    changed = True
        if changed:
            self.version += 1

    def set_all_for_column(self, col_key, value: bool):
        """Check or uncheck all valid combos in the given column."""
        changed = False
        for site in self._sites:
            depths_arr = np.asarray(site["depths"]).ravel()
            for depth_val in depths_arr:
                if self.col_dim == "vertical":
                    col_matches = (float(depth_val) == float(col_key))
                else:
                    col_matches = (str(site["site_id"]) == str(col_key))
                if not col_matches:
                    continue
                key = (str(site["site_id"]), float(depth_val))
                before = key in self._checked
                if value and not before:
                    self._checked.add(key)
                    changed = True
                elif not value and before:
                    self._checked.discard(key)
                    changed = True
        if changed:
            self.version += 1

    # ── queries ──────────────────────────────────────────────────────

    def get_selected_combinations(self):
        """Return ``list[(entity_index, depth_idx)]`` for plotting."""
        combos = []
        for site in self._sites:
            depths_arr = np.asarray(site["depths"]).ravel()
            for depth_idx, depth_val in enumerate(depths_arr):
                if (str(site["site_id"]), float(depth_val)) in self._checked:
                    combos.append((site["entity_index"], int(depth_idx)))
        return combos

    @property
    def all_depths(self) -> list[float]:
        """Sorted union of every depth value present across all sites."""
        result: set[float] = set()
        for site in self._sites:
            for d in np.asarray(site["depths"]).ravel():
                result.add(float(d))
        return sorted(result)


LBL_WIDTH = 120
CELL_WIDTH = 70
BTN_WIDTH = 40


def _format_depth(depth_value: float) -> str:
    return f"{depth_value:.2f}"


def build_table(state: SelectionState) -> pn.Column:
    """Build the site × depth checkbox matrix from *state* (legacy).

    Orientation is controlled by ``state.row_dim`` / ``state.col_dim``:

    ``row_dim="entity"``
      Rows = sites, columns = depths.  Each row has a site label, one checkbox
      per depth, and a remove button.

    ``row_dim="vertical"``
      Rows = depths, columns = sites.  Each row has a depth label and one
      checkbox per site.  No remove buttons in this orientation.
    """
    rows: list[pn.Row] = []

    is_depth_rows = state.row_dim == "vertical"

    if is_depth_rows:
        header_cells: list = [
            pn.pane.Markdown("**Depth**", width=LBL_WIDTH),
            *[
                pn.pane.Markdown(f"**{s['site_id']}**", width=CELL_WIDTH)
                for s in state.sites
            ],
        ]
        rows.append(pn.Row(*header_cells, sizing_mode="stretch_width"))

        for depth_val in state.all_depths:
            cells: list = [
                pn.pane.Markdown(f"**{_format_depth(depth_val)}**", width=LBL_WIDTH),
            ]
            for site in state.sites:
                site_id = site["site_id"]
                site_depths = set(float(d) for d in np.asarray(site["depths"]).ravel())
                if depth_val in site_depths:
                    cb = pn.widgets.Checkbox(
                        value=(str(site_id), float(depth_val)) in state._checked,
                        width=CELL_WIDTH,
                    )

                    def _on_cb(event, _site_id=site_id, _depth=depth_val):
                        state.set_selected(_site_id, _depth, event.new)

                    cb.param.watch(_on_cb, "value")
                    cells.append(cb)
                else:
                    cells.append(pn.pane.Markdown("—", width=CELL_WIDTH))

            rows.append(pn.Row(*cells, sizing_mode="stretch_width"))
    else:
        header_cells: list = [
            pn.pane.Markdown("", width=BTN_WIDTH),
            pn.pane.Markdown("**Site**", width=LBL_WIDTH),
            *[
                pn.pane.Markdown(f"**{_format_depth(d)}**", width=CELL_WIDTH)
                for d in state.all_depths
            ],
        ]
        rows.append(pn.Row(*header_cells, sizing_mode="stretch_width"))

        for site in state.sites:
            site_id = site["site_id"]
            site_depths = set(float(d) for d in np.asarray(site["depths"]).ravel())

            remove_btn = pn.widgets.Button(
                name="✕",
                width=BTN_WIDTH,
                height=30,
                button_type="danger",
                stylesheets=[".bk-btn-danger { padding: 0px !important; font-size: 12px; }"],
            )

            def _on_remove(event, _idx=site["entity_index"]):
                state.remove_site(_idx)

            remove_btn.on_click(_on_remove)

            cells: list = [
                remove_btn,
                pn.pane.Markdown(f"**{site_id}**", width=LBL_WIDTH),
            ]

            for depth_val in state.all_depths:
                if depth_val in site_depths:
                    cb = pn.widgets.Checkbox(
                        value=(str(site_id), float(depth_val)) in state._checked,
                        width=CELL_WIDTH,
                    )

                    def _on_cb(event, _site_id=site_id, _depth=depth_val):
                        state.set_selected(_site_id, _depth, event.new)

                    cb.param.watch(_on_cb, "value")
                    cells.append(cb)
                else:
                    cells.append(pn.pane.Markdown("—", width=CELL_WIDTH))

            rows.append(pn.Row(*cells, sizing_mode="stretch_width"))

    return pn.Column(*rows, sizing_mode="stretch_width")


from dashboard.plot_styles import COLORS, MARKER_SHAPES, SHAPE_TO_DASH

import pandas as pd


def resolve_available_dimensions(
    endpoint_config: dict | None = None,
    group_path: str | None = None,
    schema_display: dict | None = None,
) -> dict[str, str]:
    """Return ``{display_label: dim_key}`` for eligible coordinate dimensions.

    Excludes ``lat``, ``lon``, ``time``, ``x``, ``y``, ``z`` which are
    unsuitable as table axes.

    Parameters
    ----------
    endpoint_config : dict, optional
        Raw endpoint config dict (from ``data.client.get_endpoint()``).
    group_path : str, optional
        Current group path for field resolution.
    schema_display : dict, optional
        ``schema_display`` dict for human-readable names.

    Returns
    -------
    dict
        ``{display_label: dimension_key}``  e.g. ``{"Site": "entity", "Depth": "vertical"}``
    """
    from dashboard.config import _resolve_fields_for_group_raw

    EXCLUDED = {"lat", "lon", "time", "x", "y", "z"}

    if endpoint_config is None:
        return {"Site": "entity", "Depth": "vertical"}

    schema_config = endpoint_config.get("schema", {})
    resolved = _resolve_fields_for_group_raw(schema_config, group_path or "/")

    display = schema_display or {}
    dims: dict[str, str] = {}
    for key, field_name in resolved.items():
        if key in EXCLUDED:
            continue
        label = field_name or key
        if key == "entity" and display.get("entity_name"):
            label = display["entity_name"]
        elif key == "vertical" and display.get("vertical_name"):
            label = display["vertical_name"]
        dims[label] = key

    if not dims:
        dims = {"Site": "entity", "Depth": "vertical"}

    return dims


def build_assignment_matrix(
    selection_state: SelectionState,
    row_dim: str | None = None,
    col_dim: str | None = None,
) -> tuple[pd.DataFrame, dict, dict, dict, dict[str, str], dict[str, str]]:
    """Build a Tabulator-ready assignment matrix from *selection_state*.

    Returns
    -------
    df : pd.DataFrame
        Table data with row labels, marker shapes, and boolean assignment
        columns.  Hidden ``__valid_<col>`` columns track cell validity.
    editors : dict
        Per-column Tabulator editor config.
    formatters : dict
        Per-column Tabulator formatter config.
    editables : dict
        Per-column boolean editability flag.
    row_shapes : dict
        ``{row_key: marker_shape_name}``
    col_colors : dict
        ``{col_key: hex_color}``
    """
    if row_dim is None:
        row_dim = selection_state.row_dim
    if col_dim is None:
        col_dim = selection_state.col_dim

    # Temporarily switch state orientation so is_valid / is_checked
    # use the requested dimensions, then restore originals.
    _orig_row = selection_state.row_dim
    _orig_col = selection_state.col_dim
    selection_state.row_dim = row_dim
    selection_state.col_dim = col_dim

    row_keys = list(selection_state.row_keys)
    col_keys = list(selection_state.col_keys)

    row_shapes: dict[str, str] = {
        str(rk): MARKER_SHAPES[i % len(MARKER_SHAPES)]
        for i, rk in enumerate(row_keys)
    }
    col_colors: dict[str, str] = {
        str(ck): COLORS[i % len(COLORS)]
        for i, ck in enumerate(col_keys)
    }

    selection_state._row_shapes = row_shapes
    selection_state._col_colors = col_colors

    rows: list[dict] = []
    sites_lookup = {str(s["site_id"]): s["entity_index"] for s in selection_state.sites}
    for i, row_key in enumerate(row_keys):
        eid = sites_lookup.get(str(row_key), np.nan) if row_dim == "entity" else np.nan
        row: dict = {
            "_index": i,
            "_row_label": str(row_key),
            "_marker": row_shapes.get(str(row_key), "circle"),
            "entity_index": eid,
            "_actions": "✕",
        }
        row_any_valid = False
        for col_key in col_keys:
            col_s = str(col_key)
            valid = selection_state.is_valid(row_key, col_key)
            if valid:
                row_any_valid = True
                checked = selection_state.is_checked(row_key, col_key)
                row[col_s] = bool(checked)
                row[f"__valid_{col_s}"] = True
            else:
                row[col_s] = None
                row[f"__valid_{col_s}"] = False
        rows.append(row)

    df = pd.DataFrame(rows)
    if "_index" in df.columns:
        df = df.drop(columns=["_index"])

    selection_cols = [str(ck) for ck in col_keys]

    editors: dict = {
        "_row_label": None,
        "_marker": None,
        "_actions": None,
        **{
            col: {"type": "tickCross", "tristate": True, "indeterminateValue": None}
            for col in selection_cols
        },
    }

    formatters: dict = {
        "_row_label": {"type": "text"},
        "_marker": {"type": "text"},
        "_actions": {"type": "button", "label": "✕ Remove", "buttonType": "danger"},
        **{col: {"type": "tickCross"} for col in selection_cols},
    }

    editables: dict = {
        "_row_label": False,
        "_marker": False,
        "_actions": False,
        **{col: True for col in selection_cols},
    }

    # Restore original orientation
    selection_state.row_dim = _orig_row
    selection_state.col_dim = _orig_col

    return df, editors, formatters, editables, row_shapes, col_colors


def _build_legend_html(state):
    """Compact HTML legend showing row shapes and column colors."""
    combos = state.get_selected_combinations()
    if not combos or not state.sites:
        return "<i>No curves selected</i>"

    row_shapes = getattr(state, "_row_shapes", {})
    col_colors = getattr(state, "_col_colors", {})

    parts = ['<div style="font-size: 11px; line-height: 1.6;">']

    keys = list(col_colors.keys())
    if keys:
        parts.append('<div style="display: flex; flex-wrap: wrap; gap: 4px 10px; margin-bottom: 2px;">')
        if state.col_dim == "vertical":
            parts.append("<b>Color — Depth:</b>")
        else:
            parts.append("<b>Color — Site:</b>")
        for k in keys:
            color = col_colors.get(k, "#888")
            swatch = (
                f'<span style="display:inline-block;width:10px;height:10px;'
                f'background:{color};border-radius:2px;vertical-align:middle;"></span>'
            )
            parts.append(f"<span>{swatch} {k}</span>")
        parts.append("</div>")

    row_keys = list(row_shapes.keys())
    if row_keys:
        parts.append('<div style="display: flex; flex-wrap: wrap; gap: 4px 10px;">')
        if state.row_dim == "entity":
            parts.append("<b>Dash — Site:</b>")
        else:
            parts.append("<b>Dash — Depth:</b>")
        for k in row_keys:
            shape = row_shapes.get(k, "circle")
            dash = SHAPE_TO_DASH.get(shape, "solid")
            parts.append(f'<span><span style="display:inline-block;width:20px;height:2px;background:#aaa;vertical-align:middle;margin-right:3px;"></span> {dash} — {k}</span>')
        parts.append("</div>")

    parts.append("</div>")
    return "".join(parts)


def build_plot_selection_panel(
    state: SelectionState | None = None,
    available_dims: dict[str, str] | None = None,
    plot_var_selector: pn.widgets.Select | None = None,
) -> tuple[pn.Column, SelectionState]:
    """Build the Tabulator-based Plot Selection panel.

    Parameters
    ----------
    state : SelectionState, optional
        Reuse an existing state instance (e.g. when switching endpoints).
        If omitted a fresh state is created.
    available_dims : dict, optional
        ``{display_label: dim_key}`` from ``resolve_available_dimensions()``.
        Defaults to ``{"Site": "entity", "Depth": "vertical"}``.
    plot_var_selector : pn.widgets.Select, optional
        Variable name dropdown.  Placed at the top of the panel when provided.

    Returns
    -------
    panel : pn.Column
        The assembled widget.
    state : SelectionState
        The backing state object — wire callbacks to this.
    """
    if state is None:
        state = SelectionState()
    if available_dims is None:
        available_dims = {"Site": "entity", "Depth": "vertical"}

    row_options = available_dims
    col_options = dict(available_dims)

    row_select = pn.widgets.Select(
        name="Rows",
        options=row_options,
        value=state.row_dim,
        width=120,
    )
    col_select = pn.widgets.Select(
        name="Columns",
        options=col_options,
        value=state.col_dim,
        width=120,
    )

    df, editors, formatters, editables, _rshapes, _ccolors = build_assignment_matrix(
        state, state.row_dim, state.col_dim
    )

    hidden = [c for c in df.columns if c.startswith("__valid_") or c == "_marker" or c == "entity_index"]
    if state.row_dim != "entity":
        hidden.append("_actions")

    titles = {"_row_label": "", "_actions": "Remove"}
    table = pn.widgets.Tabulator(
        df,
        titles=titles,
        editors=editors,
        formatters=formatters,
        hidden_columns=hidden,
        frozen_columns=["_row_label", "_actions"],
        selectable=False,
        show_index=False,
        max_height=400,
        sizing_mode="stretch_width",
        layout="fit_data_table",
        theme="midnight",
        sortable=False,
    )

    _orientation_lock = False
    _updating_table = False

    def _rebuild_table():
        nonlocal _updating_table
        _updating_table = True
        try:
            new_df, new_editors, new_formatters, _, _, _ = build_assignment_matrix(
                state, state.row_dim, state.col_dim
            )
            new_hidden = [c for c in new_df.columns if c.startswith("__valid_") or c == "_marker" or c == "entity_index"]
            if state.row_dim != "entity":
                new_hidden.append("_actions")
            else:
                new_hidden = [c for c in new_hidden if c != "_actions"]
            table.titles = {"_row_label": "", "_actions": "Remove"}
            _rebuild_col_styles()
            table.value = new_df
            table.editors = new_editors
            table.formatters = new_formatters
            table.hidden_columns = new_hidden
            _rebuild_col_labels()
        finally:
            _updating_table = False

    def _sync_orientation(event=None):
        nonlocal _orientation_lock
        if _orientation_lock:
            return
        _orientation_lock = True
        try:
            if row_select.value == col_select.value:
                other = next(
                    (k for k, v in available_dims.items() if v != row_select.value),
                    None,
                )
                if other is not None:
                    col_select.value = available_dims[other]
            state.row_dim = row_select.value
            state.col_dim = col_select.value
            _rebuild_table()
            state.version += 1
        finally:
            _orientation_lock = False

    row_select.param.watch(_sync_orientation, "value")
    col_select.param.watch(_sync_orientation, "value")

    def _on_table_cell_click(event):
        """Handle clicks on action columns."""
        if event.column == "_actions":
            row_data = table.value.iloc[event.row]
            eid = row_data.get("entity_index")
            if eid is not None and not (isinstance(eid, float) and np.isnan(eid)):
                state.remove_site(int(eid))
        elif event.column == "_row_label":
            row_data = table.value.iloc[event.row]
            row_key = row_data["_row_label"]
            any_unchecked = False
            for col in table.value.columns:
                if col.startswith("__valid_") or col in ("_row_label", "_marker", "entity_index", "_actions", "_index"):
                    continue
                if row_data.get(f"__valid_{col}", False) and not row_data.get(col, False):
                    any_unchecked = True
                    break
            state.set_all_for_row(row_key, any_unchecked)
            _rebuild_table()

    table.on_click(_on_table_cell_click)

    def _on_table_edit(event):
        """Handle user edits to the Tabulator."""
        nonlocal _updating_table
        if _updating_table:
            return
        col = event.column
        row_idx = event.row
        new_value = event.value

        if col in ("_row_label", "_marker"):
            return

        row_key = table.value.iloc[row_idx]["_row_label"]

        state.set_checked(row_key, col, bool(new_value))

    table.on_edit(_on_table_edit)

    def _on_layout_change(event):
        _rebuild_table()

    state.param.watch(_on_layout_change, "layout_version")

    # ── Column toggle labels ──────────────────────────────────────
    col_label_row = pn.Row(sizing_mode="stretch_width", styles={"margin-bottom": "4px"})

    def _rebuild_col_labels():
        col_keys = list(state.col_keys)
        children = []
        for ck in col_keys:
            ck_s = str(ck)
            color = state._col_colors.get(ck_s, "#94a3b8")

            def _on_click(event, _c=ck_s):
                any_unchecked = False
                for site in state._sites:
                    for d in np.asarray(site["depths"]).ravel():
                        if state.col_dim == "vertical":
                            col_matches = (float(d) == float(_c))
                        else:
                            col_matches = (str(site["site_id"]) == str(_c))
                        key = (str(site["site_id"]), float(d))
                        if col_matches and key not in state._checked:
                            any_unchecked = True
                            break
                    if any_unchecked:
                        break
                state.set_all_for_column(_c, any_unchecked)
                _rebuild_table()

            label = pn.widgets.Button(
                name=ck_s,
                width=65,
                height=22,
                button_type="default",
                stylesheets=[f"""
                    .bk-btn-default {{
                        background: none !important;
                        border: none !important;
                        color: {color} !important;
                        font-size: 11px !important;
                        font-weight: 600 !important;
                        padding: 0 4px !important;
                        cursor: pointer !important;
                    }}
                    .bk-btn-default:hover {{
                        color: #e2e8f0 !important;
                        background: #334155 !important;
                        border-radius: 3px !important;
                    }}
                """],
            )
            label.on_click(_on_click)
            children.append(label)
        col_label_row[:] = children

    _rebuild_col_labels()

    # ── Column header colors (Tabulator) ──────────────────────────
    def _rebuild_col_styles():
        col_keys = list(state.col_keys)
        config_columns = []
        css_parts = []
        for i, ck in enumerate(col_keys):
            ck_s = str(ck)
            color = state._col_colors.get(ck_s, "#94a3b8")
            cls = f"colhdr-{i}"
            config_columns.append({"field": ck_s, "cssClass": cls})
            css_parts.append(
                f".tabulator-col.{cls} .tabulator-col-title {{ color: {color} !important; }}"
            )
            css_parts.append(
                f".tabulator-col.{cls} .tabulator-col-title:hover {{ color: #fff !important; }}"
            )
        table._configuration = {"columns": config_columns}
        table.stylesheets = ["\n".join(css_parts)]

    _rebuild_col_styles()

    control_bar = pn.Row(
        row_select,
        col_select,
        plot_var_selector or pn.Spacer(width=0),
        sizing_mode="stretch_width",
    )

    legend_pane = pn.pane.HTML("", sizing_mode="stretch_width", margin=(2, 0, 0, 0))
    legend_accordion = pn.Accordion(
        ("Legend", legend_pane),
        active=[],
        sizing_mode="stretch_width",
    )

    def _update_legend(event):
        legend_pane.object = _build_legend_html(state)

    state.param.watch(_update_legend, "version")
    _update_legend(None)

    panel = pn.Column(
        control_bar,
        col_label_row,
        table,
        legend_accordion,
        sizing_mode="stretch_width",
    )

    return panel, state



