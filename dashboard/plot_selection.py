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
