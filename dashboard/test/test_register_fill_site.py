"""Tests for SelectionState.register_site / fill_site — table-first taps."""

from __future__ import annotations

import numpy as np
import pandas as pd

from dashboard.plot_selection import SelectionState


def _make_state():
    state = SelectionState()
    state.add_site(
        entity_index=0, site_id="BH-1",
        depths=[0.0, 1.0, 2.0],
        series=[[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        times=["2020-01-01", "2020-01-02", "2020-01-03"],
    )
    return state


class TestRegisterSite:
    def test_registers_pending_row_without_bumping_version(self):
        state = _make_state()
        version_before = state.version
        layout_before = state.layout_version

        state.register_site(entity_index=5, site_id="BH-5")

        assert state.version == version_before, "plots must not redraw yet"
        assert state.layout_version > layout_before, "table must rebuild"
        assert len(state.sites) == 2
        site = state.sites[-1]
        assert site["entity_index"] == 5
        assert site["site_id"] == "BH-5"
        assert len(site["depths"]) == 0, "pending site holds no depths yet"

    def test_registered_pending_site_has_no_checked_cells(self):
        state = _make_state()
        state.register_site(entity_index=5, site_id="BH-5")
        assert state.checked_combinations() == {("BH-1", 0.0), ("BH-1", 1.0), ("BH-1", 2.0)}

    def test_duplicate_registration_is_noop(self):
        state = _make_state()
        state.register_site(entity_index=5, site_id="BH-5")
        n_before = len(state.sites)
        version_before = state.version
        layout_before = state.layout_version

        state.register_site(entity_index=5, site_id="BH-5")

        assert len(state.sites) == n_before
        assert state.version == version_before
        assert state.layout_version == layout_before


class TestFillSite:
    def test_fill_registered_site_auto_checks_depths(self):
        state = _make_state()
        state.register_site(entity_index=5, site_id="BH-5")
        version_before = state.version

        ok = state.fill_site(
            entity_index=5,
            series=[[10, 11, 12], [13, 14, 15]],
            times=pd.to_datetime(["2020-02-01", "2020-02-02", "2020-02-03"]),
            depths=[5.0, 6.0],
        )

        assert ok is True
        assert state.version > version_before, "plots must redraw on fill"
        assert ("BH-5", 5.0) in state.checked_combinations()
        assert ("BH-5", 6.0) in state.checked_combinations()

    def test_fill_bumps_layout_when_depths_grow(self):
        state = _make_state()
        state.register_site(entity_index=5, site_id="BH-5")
        layout_before = state.layout_version

        state.fill_site(
            entity_index=5,
            series=[[10, 11, 12]],
            times=pd.to_datetime(["2020-02-01", "2020-02-02", "2020-02-03"]),
            depths=[5.0, 6.0],
        )

        assert state.layout_version > layout_before, "new depth columns require table rebuild"

    def test_fill_unknown_site_returns_false(self):
        state = _make_state()
        ok = state.fill_site(
            entity_index=99,
            series=[[10, 11, 12]],
            times=pd.to_datetime(["2020-02-01", "2020-02-02", "2020-02-03"]),
            depths=[5.0],
        )
        assert ok is False

    def test_register_then_fill_preserves_previous_sites(self):
        state = _make_state()
        state.register_site(entity_index=5, site_id="BH-5")
        state.fill_site(
            entity_index=5,
            series=[[10, 11, 12]],
            times=pd.to_datetime(["2020-02-01", "2020-02-02", "2020-02-03"]),
            depths=[5.0],
        )

        assert {s["site_id"] for s in state.sites} == {"BH-1", "BH-5"}
        assert ("BH-1", 0.0) in state.checked_combinations()
        assert ("BH-5", 5.0) in state.checked_combinations()

    def test_refill_same_depths_is_idempotent(self):
        state = _make_state()
        state.register_site(entity_index=5, site_id="BH-5")
        first_fill = ([[10, 11, 12], [13, 14, 15]], [5.0, 6.0])
        state.fill_site(
            entity_index=5,
            series=first_fill[0],
            times=pd.to_datetime(["2020-02-01", "2020-02-02", "2020-02-03"]),
            depths=first_fill[1],
        )
        checked_before = state.checked_combinations()
        layout_before = state.layout_version

        ok = state.fill_site(
            entity_index=5,
            series=first_fill[0],
            times=pd.to_datetime(["2020-02-01", "2020-02-02", "2020-02-03"]),
            depths=first_fill[1],
        )

        assert ok is True
        assert state.checked_combinations() == checked_before, "no duplicate checked cells"
        assert state.layout_version == layout_before, "same-depth refill must not rebuild table"

    def test_first_fill_bumps_layout_for_new_depths(self):
        state = _make_state()
        state.register_site(entity_index=5, site_id="BH-5")
        layout_before = state.layout_version
        checked_before = state.checked_combinations()

        state.fill_site(
            entity_index=5,
            series=[[10, 11, 12]],
            times=pd.to_datetime(["2020-02-01", "2020-02-02", "2020-02-03"]),
            depths=[5.0],
        )

        assert state.layout_version > layout_before, "new depth columns require table rebuild"
        assert ("BH-5", 5.0) in state.checked_combinations()
        assert len(state.checked_combinations()) == len(checked_before) + 1