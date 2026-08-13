"""Plot selection panel — table-based site/depth selector."""

from __future__ import annotations

import threading
import time

import numpy as np
import pandas as pd
import panel as pn
import param
from panel.io.model import JSCode

from dashboard.plot_styles import COLORS, MARKER_SHAPES, SHAPE_TO_SVG


class _PerfMetrics:
    """Collects lightweight performance counters for the plot selection table.

    Reset via ``reset()``.  Read via ``report()``.
    """

    def __init__(self):
        self.reset()

    def reset(self):
        self.cell_click_count = 0
        self.full_rebuild_count = 0
        self.version_bump_count = 0
        self.edit_handler_ms: list[float] = []
        self.rebuild_duration_ms: list[float] = []

    def report(self) -> str:
        click_avg = (
            sum(self.edit_handler_ms) / len(self.edit_handler_ms)
            if self.edit_handler_ms else 0.0
        )
        rebuild_avg = (
            sum(self.rebuild_duration_ms) / len(self.rebuild_duration_ms)
            if self.rebuild_duration_ms else 0.0
        )
        return (
            f"[PerfMetrics] clicks={self.cell_click_count} "
            f"full_rebuilds={self.full_rebuild_count} "
            f"version_bumps={self.version_bump_count} "
            f"click->rebuild_avg={click_avg:.1f}ms "
            f"rebuild_avg={rebuild_avg:.1f}ms"
        )


perf = _PerfMetrics()


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
        self._valid_count: int = 0
        self._checked_count: int = 0

    def _cancel_panel_bump(self, *, clear_loading=False):
        """Cancel any pending debounced version bump scheduled by the panel.

        Called internally before direct ``version`` or ``layout_version``
        mutations to prevent a stale debounce callback from firing after
        the state has already been updated.
        """
        fn = getattr(self, "_panel_cancel_bump", None)
        if fn is not None:
            fn(clear_loading=clear_loading)

    def _bump_version(self, *, clear_loading=False):
        """Increment ``version`` after cancelling any pending debounce.

        Every method on SelectionState that directly increments
        ``self.version`` should go through this helper so that a
        stale panel debounce timer is always invalidated first.
        """
        self._cancel_panel_bump(clear_loading=clear_loading)
        inv = getattr(self, "_panel_invalidate_bump_generation", None)
        if inv is not None:
            inv()
        self.version += 1

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

    def set_checked(self, row_key, col_key, value: bool, bump_version: bool = True):
        """Mark *(row_key, col_key)* as checked or unchecked.

        Only affects valid combinations; invalid combos are silently
        ignored.  Bumps *version* on change (unless *bump_version* is
        ``False``).
        """
        if not self.is_valid(row_key, col_key):
            return
        site_id, depth_value = self._resolve_canonical(row_key, col_key)
        key = (str(site_id), float(depth_value))
        before = key in self._checked
        if value and not before:
            self._checked.add(key)
            self._checked_count += 1
        elif not value and before:
            self._checked.discard(key)
            self._checked_count -= 1
        else:
            return  # no change
        if bump_version:
            self._bump_version()

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
                self._cancel_panel_bump()
                site["depths"] = np.asarray(depths, dtype=float).ravel()
                site["series"] = series
                site["times"] = times
                self.layout_version += 1
                self._bump_version()
                print(f"[SelectionState] Updated data for site {site_id} (idx={entity_index})")
                return

        depths_arr = np.asarray(depths, dtype=float).ravel()
        finite_depths = [float(d) for d in depths_arr if np.isfinite(float(d))]

        self._sites.append({
            "entity_index": int(entity_index),
            "site_id": str(site_id),
            "depths": depths_arr,
            "series": series,
            "times": times,
        })

        for dv in finite_depths:
            key = (str(site_id), dv)
            self._checked.add(key)

        self._valid_count += len(finite_depths)
        self._checked_count += len(finite_depths)
        self.layout_version += 1
        self._bump_version()
        print(f"[SelectionState] Added site {site_id} (idx={entity_index}), "
              f"depths={depths_arr.tolist()}")

    def remove_site(self, entity_index, bump_version: bool = True, bump_layout: bool = True):
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

        depths_arr = np.asarray(site["depths"]).ravel()
        finite_count = sum(1 for d in depths_arr if np.isfinite(float(d)))
        self._valid_count -= finite_count

        for k in list(self._checked):
            if k[0] == site_id:
                self._checked.discard(k)
                self._checked_count -= 1

        if bump_layout:
            self.layout_version += 1
        if bump_version:
            self._bump_version()
        print(f"[SelectionState] Removed site {site_id} (idx={entity_index})")

    def clear(self):
        """Remove all sites and reset selection."""
        self._cancel_panel_bump(clear_loading=True)
        self._sites.clear()
        self._checked.clear()
        self._valid_count = 0
        self._checked_count = 0
        self.layout_version += 1
        self._bump_version(clear_loading=True)
        print("[SelectionState] Cleared all sites")

    def select_all(self, bump_version: bool = True, bump_layout: bool = True):
        """Check every valid (site, depth) cell."""
        changed = False
        for site in self._sites:
            for d in site["depths"]:
                dv = float(d)
                if not np.isfinite(dv):
                    continue
                k = (str(site["site_id"]), dv)
                if k not in self._checked:
                    self._checked.add(k)
                    changed = True
        if changed:
            self._checked_count = self._valid_count
            if bump_layout:
                self.layout_version += 1
            if bump_version:
                self._bump_version()

    def deselect_all(self, bump_version: bool = True, bump_layout: bool = True):
        """Uncheck every (site, depth) cell."""
        if self._checked:
            self._checked.clear()
            self._checked_count = 0
            if bump_layout:
                self.layout_version += 1
            if bump_version:
                self._bump_version()

    def set_all_for_row(self, row_key, value: bool, bump_version: bool = True, bump_layout: bool = True):
        """Check or uncheck all valid combos in the given row."""
        changed = False
        delta = 0
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
                    delta += 1
                elif not value and before:
                    self._checked.discard(key)
                    changed = True
                    delta -= 1
        if changed:
            self._checked_count += delta
            if bump_layout:
                self.layout_version += 1
            if bump_version:
                self._bump_version()

    def set_all_for_column(self, col_key, value: bool, bump_version: bool = True, bump_layout: bool = True):
        """Check or uncheck all valid combos in the given column."""
        changed = False
        delta = 0
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
                    delta += 1
                elif not value and before:
                    self._checked.discard(key)
                    changed = True
                    delta -= 1
        if changed:
            self._checked_count += delta
            if bump_layout:
                self.layout_version += 1
            if bump_version:
                self._bump_version()

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


def resolve_available_dimensions(
    view_config: dict | None = None,
    group_path: str | None = None,
    schema_display: dict | None = None,
) -> dict[str, str]:
    """Return ``{display_label: dim_key}`` for eligible coordinate dimensions.

    Excludes ``lat``, ``lon``, ``time``, ``x``, ``y``, ``z`` which are
    unsuitable as table axes.

    Parameters
    ----------
    view_config : dict, optional
        Raw view config dict (from ``data.client.get_view()``).
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

    if view_config is None:
        return {"Site": "entity", "Depth": "vertical"}

    schema_config = view_config.get("schema", {})
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


_SELECTION_FORMATTER = JSCode(r"""
function(cell, formatterParams, onRendered) {
    const data = cell.getRow().getData();

    // Top "All" row: collective column toggle.
    if (data._row_key === "All") {
        const element = document.createElement("span");
        const color = formatterParams.color || "#94a3b8";

        element.textContent = cell.getField();
        element.style.color = color;
        element.style.fontWeight = "bold";
        element.style.cursor = "pointer";
        element.style.display = "block";
        element.style.width = "100%";

        element.addEventListener("click", function(event) {
            event.preventDefault();
            event.stopPropagation();

            const table = cell.getTable();
            const field = cell.getField();
            const rows = table.getRows();

            // Determine the next value from current browser-side data.
            let targetValue = false;

            for (const row of rows) {
                const rowData = row.getData();

                if (
                    rowData._row_key !== "All" &&
                    rowData["__valid_" + field] === true &&
                    rowData[field] !== true
                ) {
                    targetValue = true;
                    break;
                }
            }

            // Update synchronously. setValue triggers Panel's on_edit callback.
            for (const row of rows) {
                const rowData = row.getData();

                if (
                    rowData._row_key === "All" ||
                    rowData["__valid_" + field] !== true
                ) {
                    continue;
                }

                const targetCell = row.getCell(field);

                if (
                    targetCell &&
                    targetCell.getValue() !== targetValue
                ) {
                    targetCell.setValue(targetValue, false);
                }
            }
        });

        return element;
    }

    const value = cell.getValue();

    // Invalid cells remain empty and non-clickable.
    if (value !== true && value !== false) {
        return "";
    }

    const element = document.createElement("span");

    element.innerHTML = value ? "&#10003;" : "&#10007;";
    element.style.color = value ? "#10b981" : "#64748b";
    element.style.cursor = "pointer";
    element.style.display = "flex";
    element.style.alignItems = "center";
    element.style.justifyContent = "center";
    element.style.width = "100%";
    element.style.height = "100%";
    element.style.boxSizing = "border-box";

    element.addEventListener("click", function(event) {
        // Prevent the normal cell-click path and native editor.
        event.preventDefault();
        event.stopPropagation();

        // Immediate browser-side value and icon update.
        cell.setValue(cell.getValue() !== true, false);
    });

    return element;
}
""")

_ROW_LABEL_FORMATTER = JSCode(r"""
function(cell, formatterParams, onRendered) {
    const element = document.createElement("span");

    element.innerHTML = cell.getValue() || "";
    element.style.cursor = "pointer";
    element.style.display = "block";
    element.style.width = "100%";

    element.addEventListener("click", function(event) {
        event.preventDefault();
        event.stopPropagation();

        const table = cell.getTable();
        const clickedRow = cell.getRow();
        const clickedData = clickedRow.getData();

        const ignoredFields = new Set([
            "_index",
            "_actions",
            "_row_label",
            "_row_key",
            "entity_index",
        ]);

        const selectionFields = table
            .getColumns()
            .map(function(column) {
                return column.getField();
            })
            .filter(function(field) {
                return (
                    field &&
                    !ignoredFields.has(field) &&
                    !field.startsWith("__valid_")
                );
            });

        // Global All toggle.
        if (clickedData._row_key === "All") {
            const rows = table.getRows();
            let targetValue = false;

            outer:
            for (const row of rows) {
                const rowData = row.getData();

                if (rowData._row_key === "All") {
                    continue;
                }

                for (const field of selectionFields) {
                    if (
                        rowData["__valid_" + field] === true &&
                        rowData[field] !== true
                    ) {
                        targetValue = true;
                        break outer;
                    }
                }
            }

            for (const row of rows) {
                const rowData = row.getData();

                if (rowData._row_key === "All") {
                    continue;
                }

                for (const field of selectionFields) {
                    if (rowData["__valid_" + field] !== true) {
                        continue;
                    }

                    const targetCell = row.getCell(field);

                    if (
                        targetCell &&
                        targetCell.getValue() !== targetValue
                    ) {
                        targetCell.setValue(targetValue, false);
                    }
                }
            }

            return;
        }

        // Single row toggle.
        let targetValue = false;

        for (const field of selectionFields) {
            if (
                clickedData["__valid_" + field] === true &&
                clickedData[field] !== true
            ) {
                targetValue = true;
                break;
            }
        }

        for (const field of selectionFields) {
            if (clickedData["__valid_" + field] !== true) {
                continue;
            }

            const targetCell = clickedRow.getCell(field);

            if (
                targetCell &&
                targetCell.getValue() !== targetValue
            ) {
                targetCell.setValue(targetValue, false);
            }
        }
    });

    return element;
}
""")


def _hidden_columns(df: pd.DataFrame, row_dim: str) -> list[str]:
    """Return list of columns to hide in the Tabulator widget."""
    hidden = [
        c
        for c in df.columns
        if c.startswith("__valid_") or c in ("entity_index", "_row_key")
    ]
    if row_dim != "entity":
        hidden.append("_actions")
    return hidden


def build_assignment_matrix(
    selection_state: SelectionState,
    row_dim: str | None = None,
    col_dim: str | None = None,
) -> tuple[pd.DataFrame, dict, dict, dict[str, str], dict[str, str]]:
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
    original_row_dim = selection_state.row_dim
    original_col_dim = selection_state.col_dim
    selection_state.row_dim = row_dim
    selection_state.col_dim = col_dim

    try:
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

        # ── Header row (row 0) — column labels ──
        header_row: dict = {
            "_actions": "",
            "_row_label": "All",
            "_row_key": "All",
            "entity_index": np.nan,
        }
        for col_key in col_keys:
            col_s = str(col_key)
            header_row[col_s] = None
            header_row[f"__valid_{col_s}"] = False
        rows.append(header_row)

        for row_key in row_keys:
            eid = sites_lookup.get(str(row_key), np.nan) if row_dim == "entity" else np.nan
            shape_name = row_shapes.get(str(row_key), "circle")
            row: dict = {
                "_actions": "\u2715",
                "_row_label": f"{SHAPE_TO_SVG.get(shape_name, shape_name)} {str(row_key)}",
                "_row_key": str(row_key),
                "entity_index": eid,
            }
            for col_key in col_keys:
                col_s = str(col_key)
                valid = selection_state.is_valid(row_key, col_key)
                if valid:
                    checked = selection_state.is_checked(row_key, col_key)
                    row[col_s] = bool(checked)
                    row[f"__valid_{col_s}"] = True
                else:
                    row[col_s] = None
                    row[f"__valid_{col_s}"] = False
            rows.append(row)

        df = pd.DataFrame(rows)

        selection_cols = [str(ck) for ck in col_keys]

        editors: dict = {
            "_row_label": None,
            "_actions": None,
            **{col: {"type": "tickCross"} for col in selection_cols},
        }

        formatters: dict = {
            "_row_label": {"type": _ROW_LABEL_FORMATTER},
            "_actions": {"type": "button", "label": "\u2715 Remove", "buttonType": "danger"},
            **{col: {"type": _SELECTION_FORMATTER, "color": col_colors[col]} for col in selection_cols},
        }

        return df, editors, formatters, row_shapes, col_colors
    finally:
        selection_state.row_dim = original_row_dim
        selection_state.col_dim = original_col_dim


def build_plot_selection_panel(
    state: SelectionState | None = None,
    available_dims: dict[str, str] | None = None,
    plot_var_selector: pn.widgets.Select | None = None,
    table_loading: pn.Row | None = None,
    _bump_delay_ms: float = 150,
) -> tuple[pn.Column, SelectionState]:
    """Build the Tabulator-based Plot Selection panel.

    Parameters
    ----------
    state : SelectionState, optional
        Reuse an existing state instance (e.g. when switching views).
        If omitted a fresh state is created.
    available_dims : dict, optional
        ``{display_label: dim_key}`` from ``resolve_available_dimensions()``.
        Defaults to ``{"Site": "entity", "Depth": "vertical"}``.
    plot_var_selector : pn.widgets.Select, optional
        Variable name dropdown.  Placed at the top of the panel when provided.
    table_loading : pn.Row, optional
        Loading indicator Row whose ``visible`` property is set toggled
        during table rebuilds.  When provided the built-in ``table.loading``
        overlay is suppressed in favour of this external indicator.
    _bump_delay_ms : float, optional
        Trailing debounce delay in milliseconds before bumping ``state.version``
        after a selection change.  Defaults to 150.  Exposed for testing.

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

    df, editors, formatters, _rshapes, _ccolors = build_assignment_matrix(
        state, state.row_dim, state.col_dim
    )

    hidden = _hidden_columns(df, state.row_dim)

    titles = {"_row_label": "", "_actions": "Remove"}
    table = pn.widgets.Tabulator(
        df,
        titles=titles,
        editors=editors,
        formatters=formatters,
        hidden_columns=hidden,
        frozen_columns=["_actions", "_row_label"],
        selectable=False,
        show_index=False,
        max_height=400,
        sizing_mode="stretch_width",
        layout="fit_data_table",
        theme="midnight",
        sortable=False,
    )

    _orientation_lock = False
    _bump_timer = None
    _bump_generation = 0

    def _rebuild_table():
        perf.full_rebuild_count += 1
        t0 = time.perf_counter()
        try:
            new_df, new_editors, new_formatters, _, _ = build_assignment_matrix(
                state, state.row_dim, state.col_dim
            )
            new_hidden = _hidden_columns(new_df, state.row_dim)
            _rebuild_col_styles()
            table.value = new_df
            table.editors = new_editors
            table.formatters = new_formatters
            table.hidden_columns = new_hidden
        finally:
            elapsed = (time.perf_counter() - t0) * 1000
            perf.rebuild_duration_ms.append(elapsed)
            print(f"[perf] _rebuild_table #{perf.full_rebuild_count}: {elapsed:.1f}ms")

    def _cancel_bump_timer(*, clear_loading=False):
        nonlocal _bump_timer
        if _bump_timer is not None:
            doc = pn.state.curdoc
            if doc is not None:
                try:
                    doc.remove_timeout_callback(_bump_timer)
                except ValueError:
                    pass
            elif isinstance(_bump_timer, threading.Timer):
                _bump_timer.cancel()
            _bump_timer = None
        if clear_loading and table_loading is not None:
            table_loading.visible = False

    def _schedule_bump():
        nonlocal _bump_generation, _bump_timer
        _bump_generation += 1
        gen = _bump_generation
        _cancel_bump_timer()

        def _run():
            _flush_bump(generation=gen)

        doc = pn.state.curdoc
        if doc is not None:
            _bump_timer = doc.add_timeout_callback(_run, _bump_delay_ms)
        else:
            _bump_timer = threading.Timer(_bump_delay_ms / 1000.0, _run)
            _bump_timer.daemon = True
            _bump_timer.start()

    def _flush_bump(generation=None):
        nonlocal _bump_timer
        if generation is not None and generation != _bump_generation:
            return
        _bump_timer = None
        perf.version_bump_count += 1
        state.version += 1
        if table_loading is not None:
            table_loading.visible = False
        print(f"[perf] _flush_bump -> version={state.version} (total bumps={perf.version_bump_count})")
        print(perf.report())

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
            _cancel_bump_timer(clear_loading=True)
            _rebuild_table()
            state.version += 1
        finally:
            _orientation_lock = False

    row_select.param.watch(_sync_orientation, "value")
    col_select.param.watch(_sync_orientation, "value")

    def _on_table_cell_click(event):
        """Handle clicks on remove actions only.

        All selection toggling (individual cells, column toggles, row toggles,
        global toggle) is handled by the JavaScript formatters using
        ``cell.setValue()`` which triggers ``_on_edit_cell`` via Panel's
        ``cellEdited`` synchronization.
        """
        col = event.column
        row_idx = event.row
        perf.cell_click_count += 1

        if col in ("entity_index", "_index") or col.startswith("__valid_"):
            return

        # Header row — handled by JavaScript formatters.
        if row_idx == 0:
            return

        row_data = table.value.iloc[row_idx]

        # ── Remove action ──
        if col == "_actions":
            eid = row_data.get("entity_index")
            if eid is not None and not (isinstance(eid, float) and np.isnan(eid)):
                state.remove_site(int(eid), bump_version=False)
            _schedule_bump()
            return

        # Row-label and selection cells — handled by JavaScript formatters.
        return

    table.on_click(_on_table_cell_click)

    def _on_edit_cell(event):
        """Handle Boolean edits from the browser.

        The browser updates the cell display immediately via Tabulator.js.
        This callback synchronises SelectionState and schedules the debounced
        plot redraw.  No manual ``table.patch()`` is required because Panel
        synchronizes the edited value back to ``table.value``.
        """
        col = event.column
        if col.startswith("__valid_") or col in ("entity_index", "_index",
                                                   "_row_label", "_actions"):
            return
        if table_loading is not None:
            table_loading.visible = True

        row_idx = event.row
        if row_idx == 0:
            if table_loading is not None:
                table_loading.visible = False
            return
        row_data = table.value.iloc[row_idx]

        row_key = row_data["_row_key"]

        raw_value = event.value
        if isinstance(raw_value, str):
            normalized = raw_value.strip().lower()
            if normalized == "true":
                new_value = True
            elif normalized == "false":
                new_value = False
            else:
                if table_loading is not None:
                    table_loading.visible = False
                return
        else:
            new_value = bool(raw_value)

        t_edit = time.perf_counter()

        current = state.is_checked(row_key, col)
        if current == new_value:
            if table_loading is not None:
                table_loading.visible = False
            return

        state.set_checked(row_key, col, new_value, bump_version=False)

        _schedule_bump()
        edit_elapsed = (time.perf_counter() - t_edit) * 1000
        perf.edit_handler_ms.append(edit_elapsed)
        print(f"[perf] on_edit ({row_key},{col})={new_value}: "
              f"{edit_elapsed:.1f}ms (state->schedule, no Bokeh round-trip)")

    table.on_edit(_on_edit_cell)

    def _on_layout_change(_event):
        _rebuild_table()

    state.param.watch(_on_layout_change, "layout_version")

    # ── Column config (Tabulator) ─────────────────────────────────
    def _rebuild_col_styles():
        col_keys_list = list(state.col_keys)
        config_columns = [
            {"field": "_actions", "width": 24, "headerSort": False},
            {"field": "_row_label", "width": 120, "headerSort": False},
        ]
        for ck in col_keys_list:
            ck_s = str(ck)
            config_columns.append({
                "field": ck_s,
                "width": 65,
                "headerSort": False,
                "editable": False,
            })
        table._configuration = {"headerVisible": False, "columns": config_columns}

    # ── Header row cell styles ────────────────────────────────────
    _rebuild_col_styles()
    table.stylesheets = ["""
.tabulator-row-0 {
    background: #1e293b !important;
}

.tabulator-row-0 .tabulator-cell[data-field="_row_label"] {
    cursor: pointer !important;
}
"""]

    control_bar = pn.Row(
        row_select,
        col_select,
        plot_var_selector or pn.Spacer(width=0),
        sizing_mode="stretch_width",
    )

    panel = pn.Column(
        control_bar,
        table,
        sizing_mode="stretch_width",
    )

    state._panel_schedule_bump = _schedule_bump
    state._panel_cancel_bump = _cancel_bump_timer
    state._panel_flush_bump = _flush_bump
    state._panel_bump_generation = lambda: _bump_generation

    def _invalidate_bump_generation():
        nonlocal _bump_generation
        _bump_generation += 1

    state._panel_invalidate_bump_generation = _invalidate_bump_generation

    return panel, state



