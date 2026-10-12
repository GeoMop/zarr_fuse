"""Regression tests for the variable-switch multi-site in-place refresh.

On a variable change, every selected site must be refetched by its stored
``entity_index`` directly and its data replaced in place — not just the
default site.  These tests exercise the invariant the fix relies on: a
``SelectionState`` holding multiple, non-contiguous entity indices is
refreshed via ``update_site_data`` (no ``clear()`` + re-add), preserving all
sites, their entity_index keys, table position and checked depths.
"""

from __future__ import annotations

from dashboard.plot_selection import SelectionState


_SITE_NAMES = ["BH-A", "BH-B", "BH-C"]


def _add_sites(state: SelectionState, entity_indices: list[int]) -> None:
    for ei, site_id in zip(entity_indices, _SITE_NAMES):
        state.add_site(
            entity_index=ei, site_id=site_id,
            depths=[0.0, 1.0, 2.0],
            series=[[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            times=["2020-01-01", "2020-01-02", "2020-01-03"],
        )


def _refetch_inplace(state: SelectionState, new_series) -> None:
    """Mirror _refetch_sites: update each registered site's data in place.

    The site is never cleared; its identity, checked depths and position are
    preserved.  ``new_series`` is indexed by position in ``state.sites``.
    """
    for site in list(state.sites):
        state.update_site_data(
            entity_index=site["entity_index"],
            series=new_series[site["entity_index"]],
            times=["2020-02-01", "2020-02-02", "2020-02-03"],
            depths=site["depths"],
        )


class TestMultiSiteInPlaceRefresh:
    def test_all_sites_refreshed_in_place(self):
        state = SelectionState()
        _add_sites(state, [0, 1, 2])
        saved_checked = state.checked_combinations()
        before = [(s["entity_index"], s["site_id"]) for s in state.sites]
        new_series = {
            ei: [[10] * 3, [20] * 3, [30] * 3]
            for ei in [0, 1, 2]
        }

        _refetch_inplace(state, new_series)

        assert [(s["entity_index"], s["site_id"]) for s in state.sites] == before
        # Every site's data moved to the new series (no stale old data anywhere).
        assert all(
            s["series"][0][0] == 10 and s["series"][2][0] == 30
            for s in state.sites
        )
        # Checks preserved even though the site was never cleared.
        assert state.checked_combinations() == saved_checked

    def test_non_contiguous_indices_refreshed_in_place(self):
        # Non-contiguous entity_index values, e.g. after coordinate filtering.
        state = SelectionState()
        _add_sites(state, [2, 7, 15])
        saved_checked = state.checked_combinations()
        before = {s["entity_index"] for s in state.sites}
        new_series = {ei: [[10] * 3, [20] * 3, [30] * 3] for ei in [2, 7, 15]}

        _refetch_inplace(state, new_series)

        assert {s["entity_index"] for s in state.sites} == before
        assert state.checked_combinations() == saved_checked
        assert all(s["series"][0][0] == 10 for s in state.sites)

    def test_position_stable_keyed_by_entity_index(self):
        # In-place refresh keys by the semantic entity_index, not table
        # position, so no site is dropped or reordered regardless of index
        # values / insertion order.
        state = SelectionState()
        _add_sites(state, [2, 7, 15])
        saved_checked = state.checked_combinations()
        order_before = [(s["entity_index"], s["site_id"]) for s in state.sites]

        _refetch_inplace(state, {ei: [[5] * 3, [6] * 3, [7] * 3] for ei in [2, 7, 15]})

        assert [(s["entity_index"], s["site_id"]) for s in state.sites] == order_before
        assert state.checked_combinations() == saved_checked
