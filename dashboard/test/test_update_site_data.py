"""Tests for SelectionState.update_site_data — in-place variable refresh."""

from __future__ import annotations

import numpy as np

from dashboard.plot_selection import SelectionState


def _make_state():
    state = SelectionState()
    state.add_site(
        entity_index=0, site_id="BH-1",
        depths=[0.0, 1.0, 2.0],
        series=[[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        times=["2020-01-01", "2020-01-02", "2020-01-03"],
    )
    state.add_site(
        entity_index=1, site_id="BH-2",
        depths=[1.0, 2.0, 3.0],
        series=[[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        times=["2020-01-01", "2020-01-02", "2020-01-03"],
    )
    return state


class TestUpdateSiteData:
    def test_replaces_data_in_place_keeps_identity(self):
        state = _make_state()
        before = [(s["entity_index"], s["site_id"]) for s in state.sites]
        checked_before = state.checked_combinations()

        ok = state.update_site_data(
            entity_index=0,
            series=[[10, 11, 12], [13, 14, 15], [16, 17, 18]],
            times=["2020-02-01", "2020-02-02", "2020-02-03"],
            depths=[0.0, 1.0, 2.0],
        )

        assert ok is True
        assert [(s["entity_index"], s["site_id"]) for s in state.sites] == before
        assert state.sites[0]["series"][0][0] == 10
        assert state.sites[0]["times"][0] == "2020-02-01"
        assert state.checked_combinations() == checked_before

    def test_same_depths_bumps_version_only_not_layout(self):
        # On a plain variable change the depth set is unchanged, so the table
        # must NOT rebuild (layout_version stable) while plots redraw (version).
        state = _make_state()
        v0, l0 = state.version, state.layout_version

        state.update_site_data(
            entity_index=1,
            series=[[0] * 3, [0] * 3, [0] * 3],
            times=["2020-02-01", "2020-02-02", "2020-02-03"],
            depths=[1.0, 2.0, 3.0],
        )

        assert state.version > v0
        assert state.layout_version == l0

    def test_changed_depths_bump_layout_and_prune_checks(self):
        # If a refetch changes the depth set, the table columns change, so
        # layout_version must bump and vanished checked combos are pruned.
        state = _make_state()
        # BH-1 originally depths [0,1,2]; new variable exposes only [1,2].
        assert ("BH-1", 0.0) in state.checked_combinations()
        v0, l0 = state.version, state.layout_version

        state.update_site_data(
            entity_index=0,
            series=[[1, 2, 3], [4, 5, 6]],
            times=["2020-02-01", "2020-02-02", "2020-02-03"],
            depths=[1.0, 2.0],
        )

        assert state.layout_version > l0
        assert state.version > v0
        assert ("BH-1", 0.0) not in state.checked_combinations()
        assert ("BH-1", 1.0) in state.checked_combinations()
        assert ("BH-1", 2.0) in state.checked_combinations()

    def test_unknown_entity_returns_false(self):
        state = _make_state()
        ok = state.update_site_data(
            entity_index=99,
            series=[[1, 2, 3]],
            times=["2020-02-01"],
            depths=[1.0],
        )
        assert ok is False

    def test_all_nan_series_preserves_combos_and_bumps_version(self):
        # A variable whose data is entirely NaN must still replace the site's
        # data, keep every checked combo intact, and bump version so the plots
        # re-render (blank) instead of retaining stale old-variable data.
        state = _make_state()
        checked_before = state.checked_combinations()
        v0 = state.version
        nan_series = [
            np.array([np.nan, np.nan, np.nan]),
            np.array([np.nan, np.nan, np.nan]),
            np.array([np.nan, np.nan, np.nan]),
        ]

        ok = state.update_site_data(
            entity_index=0,
            series=nan_series,
            times=["2020-02-01", "2020-02-02", "2020-02-03"],
            depths=[0.0, 1.0, 2.0],
        )

        assert ok is True
        assert state.version > v0
        assert state.checked_combinations() == checked_before
        assert np.all(np.isnan(state.sites[0]["series"][0]))

    def test_counts_stay_consistent_after_depth_change(self):
        # _valid_count / _checked_count must reflect the pruned depth set.
        state = _make_state()
        state.update_site_data(
            entity_index=0,
            series=[[1, 2, 3], [4, 5, 6]],
            times=["2020-02-01", "2020-02-02", "2020-02-03"],
            depths=[1.0, 2.0],
        )
        depths = np.asarray(state.sites[0]["depths"]).ravel()
        finite = sum(1 for d in depths if np.isfinite(float(d)))
        # BH-1 now has `finite` depths while BH-2 keeps its 3 depths.
        assert state._valid_count == 3 + finite
        assert state._checked_count == len(state._checked)


class TestUpdateSitesData:
    def test_batch_bumps_version_once_for_all_sites(self):
        state = _make_state()
        checked_before = state.checked_combinations()
        identities = [(s["entity_index"], s["site_id"]) for s in state.sites]
        v0, l0 = state.version, state.layout_version
        times = ["2020-03-01", "2020-03-02", "2020-03-03"]
        updates = [
            (0, {
                "series": [[10] * 3, [20] * 3, [30] * 3],
                "times": times,
                "depths": [0.0, 1.0, 2.0],
            }),
            (1, {
                "series": [[40] * 3, [50] * 3, [60] * 3],
                "times": times,
                "depths": [1.0, 2.0, 3.0],
            }),
        ]

        failed = state.update_sites_data(updates)

        assert failed == []
        assert state.version == v0 + 1
        assert state.layout_version == l0
        assert [(s["entity_index"], s["site_id"]) for s in state.sites] == identities
        assert state.checked_combinations() == checked_before
        assert [s["series"][0][0] for s in state.sites] == [10, 40]

    def test_none_payload_clears_data_but_preserves_depths_and_checks(self):
        state = _make_state()
        checked_before = state.checked_combinations()
        depths_before = [list(s["depths"]) for s in state.sites]
        v0, l0 = state.version, state.layout_version

        failed = state.update_sites_data([(1, None)])

        assert failed == [1]
        assert state.version == v0 + 1
        assert state.layout_version == l0
        assert state.sites[1]["series"] == []
        assert len(state.sites[1]["times"]) == 0
        assert list(state.sites[1]["depths"]) == depths_before[1]
        assert state.checked_combinations() == checked_before

    def test_changed_depths_bump_layout_once_for_batch(self):
        state = _make_state()
        v0, l0 = state.version, state.layout_version
        times = ["2020-03-01", "2020-03-02", "2020-03-03"]
        updates = [
            (0, {
                "series": [[10] * 3, [20] * 3],
                "times": times,
                "depths": [1.0, 2.0],
            }),
            (1, {
                "series": [[30] * 3, [40] * 3],
                "times": times,
                "depths": [2.0, 3.0],
            }),
        ]

        state.update_sites_data(updates)

        assert state.version == v0 + 1
        assert state.layout_version == l0 + 1
