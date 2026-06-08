"""Plot selection panel — table-based site/depth selector."""

import numpy as np
import panel as pn
import param


class SelectionState(param.Parameterized):
    """Canonical selection state for the plot selection table.

    Drives both the visible table (via *layout_version*) and the timeseries
    plots (via *version*).  The two version counters let callers distinguish
    between "rebuild the widget tree" and "just redraw plots".
    """

    version = param.Integer(default=0)
    layout_version = param.Integer(default=0)
    orientation = param.ObjectSelector(
        default="site_rows",
        objects=["site_rows", "depth_rows"],
    )

    def __init__(self, entity_field="entity", vertical_field="vertical", **params):
        super().__init__(**params)
        self.entity_field = entity_field
        self.vertical_field = vertical_field
        self._sites: list[dict] = []
        self._selected: dict[tuple[str, float], bool] = {}

    # ── read-only view of internal data ──────────────────────────────

    @property
    def sites(self) -> list[dict]:
        """Snapshot of currently registered sites."""
        return list(self._sites)

    # ── mutations ────────────────────────────────────────────────────

    def add_site(self, entity_index, site_id, depths, series, times):
        """Register a new site (or skip if already present).

        All of its depth levels are auto-selected by default.
        Triggers a table rebuild (**layout_version*).
        """
        for site in self._sites:
            if site["entity_index"] == entity_index:
                print(f"[SelectionState] Site {site_id} (idx={entity_index}) already added, skipping")
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
            key = (str(site_id), float(d))
            self._selected.setdefault(key, True)

        self.layout_version += 1
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

        for k in list(self._selected):
            if k[0] == site_id:
                del self._selected[k]

        self.layout_version += 1
        self.version += 1
        print(f"[SelectionState] Removed site {site_id} (idx={entity_index})")

    def set_selected(self, site_id, depth_value, value):
        """Check/uncheck a single (site, depth) cell.  Triggers plot re-render."""
        self._selected[(str(site_id), float(depth_value))] = bool(value)
        self.version += 1

    def select_all(self):
        """Check every (site, depth) cell."""
        changed = False
        for site in self._sites:
            for d in site["depths"]:
                k = (site["site_id"], float(d))
                if not self._selected.get(k, False):
                    self._selected[k] = True
                    changed = True
        if changed:
            self.version += 1

    def deselect_all(self):
        """Uncheck every (site, depth) cell."""
        for k in self._selected:
            self._selected[k] = False
        self.version += 1

    # ── queries ──────────────────────────────────────────────────────

    def get_selected_combinations(self):
        """Return ``list[(entity_index, depth_idx)]`` for plotting."""
        combos = []
        for site in self._sites:
            depths_arr = np.asarray(site["depths"]).ravel()
            for depth_idx, depth_val in enumerate(depths_arr):
                if self._selected.get((site["site_id"], float(depth_val)), False):
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
    """Build the site × depth checkbox matrix from *state*.

    Returns a ``pn.Column`` of rows.  Each row is a ``pn.Row`` with:
      - site name label
      - one checkbox per depth level (or ``—`` placeholder)
      - remove button
    """
    rows: list[pn.Row] = []

    # ── header row ────────────────────────────────────────────────
    header_cells: list = [
        pn.pane.Markdown("**Site**", width=LBL_WIDTH),
        *[
            pn.pane.Markdown(f"**{_format_depth(d)}**", width=CELL_WIDTH)
            for d in state.all_depths
        ],
        pn.pane.Markdown("", width=BTN_WIDTH),
    ]
    rows.append(pn.Row(*header_cells, sizing_mode="stretch_width"))

    # ── data rows ─────────────────────────────────────────────────
    for site in state.sites:
        site_id = site["site_id"]
        site_depths = set(float(d) for d in np.asarray(site["depths"]).ravel())

        cells: list = [
            pn.pane.Markdown(f"**{site_id}**", width=LBL_WIDTH),
        ]

        for depth_val in state.all_depths:
            if depth_val in site_depths:
                cb = pn.widgets.Checkbox(
                    value=state._selected.get((site_id, depth_val), False),
                    width=CELL_WIDTH,
                )

                def _on_cb(event, _site_id=site_id, _depth=depth_val):
                    state.set_selected(_site_id, _depth, event.new)

                cb.param.watch(_on_cb, "value")
                cells.append(cb)
            else:
                cells.append(pn.pane.Markdown("—", width=CELL_WIDTH))

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
        cells.append(remove_btn)

        rows.append(pn.Row(*cells, sizing_mode="stretch_width"))

    return pn.Column(*rows, sizing_mode="stretch_width")


def build_plot_selection_panel(
    entity_label="Site",
    vertical_label="Depth",
    state=None,
) -> tuple[pn.Column, SelectionState]:
    """Build the Plot Selection panel with row/column dropdowns + table.

    Parameters
    ----------
    entity_label : str
        Display name for the entity/site dimension (e.g. ``"Borehole"``).
    vertical_label : str
        Display name for the vertical/depth dimension (e.g. ``"Depth"``).
    state : SelectionState, optional
        Reuse an existing state instance (e.g. when switching endpoints).
        If omitted a fresh state is created.

    Returns
    -------
    panel : pn.Column
        The assembled widget.
    state : SelectionState
        The backing state object — wire callbacks to this.
    """
    if state is None:
        state = SelectionState(entity_field=entity_label, vertical_field=vertical_label)

    row_select = pn.widgets.Select(
        name="Rows",
        options={entity_label: "entity", vertical_label: "vertical"},
        value="entity",
        width=200,
    )
    col_select = pn.widgets.Select(
        name="Columns",
        options={vertical_label: "vertical", entity_label: "entity"},
        value="vertical",
        width=200,
    )

    _orientation_lock = False

    def _sync_orientation(event=None):
        nonlocal _orientation_lock
        if _orientation_lock:
            return
        _orientation_lock = True
        try:
            if row_select.value == col_select.value:
                other = "vertical" if row_select.value == "entity" else "entity"
                col_select.value = other
            orient = "site_rows" if row_select.value == "entity" else "depth_rows"
            state.orientation = orient
            new_table = build_table(state)
            table_area.objects = list(new_table.objects)
        finally:
            _orientation_lock = False

    row_select.param.watch(_sync_orientation, "value")
    col_select.param.watch(_sync_orientation, "value")

    def _on_layout_change(event):
        new_table = build_table(state)
        table_area.objects = list(new_table.objects)

    state.param.watch(_on_layout_change, "layout_version")

    controls = pn.Row(row_select, col_select, sizing_mode="stretch_width")
    table_area = build_table(state)

    panel = pn.Column(
        pn.pane.Markdown("**Plot Selection**", margin=(0, 0, 5, 0)),
        controls,
        table_area,
        sizing_mode="stretch_width",
    )

    return panel, state


def build_plot_selection():
    """Temporary relocation: just wraps the existing depth controls."""
    depth_selector = pn.widgets.CheckBoxGroup(
        name="Depths (m)",
        options=[],
        value=[],
        inline=False,
        sizing_mode="stretch_width",
    )
    borehole_info = pn.pane.Markdown("### No borehole selected", sizing_mode="stretch_width")
    return depth_selector, borehole_info
