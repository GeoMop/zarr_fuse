"""Unit tests for SelectionState — canonical selection store."""

from __future__ import annotations

import numpy as np
import pytest

from dashboard.plot_selection import SelectionState


def _make_state():
    """Helper: return a SelectionState with two sites pre-loaded."""
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


class TestAddSite:
    def test_add_site_auto_checks_all_depths(self):
        state = _make_state()
        assert ("BH-1", 0.0) in state._checked
        assert ("BH-1", 1.0) in state._checked
        assert ("BH-1", 2.0) in state._checked
        assert ("BH-2", 1.0) in state._checked
        assert ("BH-2", 3.0) in state._checked
        assert len(state._checked) == 6  # 3 + 3

    def test_add_duplicate_site_skipped(self):
        state = _make_state()
        n_sites_before = len(state._sites)
        n_checked_before = len(state._checked)
        state.add_site(
            entity_index=0, site_id="BH-1",
            depths=[0.0],
            series=[[1]],
            times=["2020-01-01"],
        )
        assert len(state._sites) == n_sites_before
        assert len(state._checked) == n_checked_before


class TestRemoveSite:
    def test_remove_site_drops_checked(self):
        state = _make_state()
        state.remove_site(0)
        assert len(state._sites) == 1
        assert state._sites[0]["site_id"] == "BH-2"
        assert all(k[0] != "BH-1" for k in state._checked)
        assert all(k[0] == "BH-2" for k in state._checked)

    def test_remove_nonexistent_site_noop(self):
        state = _make_state()
        n = len(state._sites)
        state.remove_site(999)
        assert len(state._sites) == n


class TestCheckedAPI:
    def test_set_checked_roundtrip(self):
        state = _make_state()
        assert state.is_checked("BH-1", 0.0) is True
        state.set_checked("BH-1", 0.0, False)
        assert state.is_checked("BH-1", 0.0) is False
        state.set_checked("BH-1", 0.0, True)
        assert state.is_checked("BH-1", 0.0) is True

    def test_set_checked_invalid_combo_silent(self):
        state = _make_state()
        state.set_checked("BH-1", 99.0, True)
        assert state.is_checked("BH-1", 99.0) is None


class TestIsValid:
    def test_valid_combo(self):
        state = _make_state()
        assert state.is_valid("BH-1", 0.0) is True
        assert state.is_valid("BH-2", 3.0) is True

    def test_invalid_depth_for_site(self):
        state = _make_state()
        assert state.is_valid("BH-1", 99.0) is False
        assert state.is_valid("BH-2", 0.0) is False  # BH-2 has no depth 0.0

    def test_invalid_site(self):
        state = _make_state()
        assert state.is_valid("NONEXISTENT", 0.0) is False


class TestCanonicalResolution:
    def test_entity_rows(self):
        """row_dim='entity' → (row_key, col_key) = (site_id, depth)."""
        state = _make_state()
        state.row_dim = "entity"
        state.col_dim = "vertical"
        site_id, depth = state._resolve_canonical("BH-1", 1.5)
        assert site_id == "BH-1"
        assert depth == 1.5

    def test_depth_rows(self):
        """row_dim='vertical' → (row_key, col_key) = (depth, site_id)
        so canonical is (col_key, row_key)."""
        state = _make_state()
        state.row_dim = "vertical"
        state.col_dim = "entity"
        site_id, depth = state._resolve_canonical(1.5, "BH-1")
        assert site_id == "BH-1"
        assert depth == 1.5

    def test_is_checked_works_in_both_orientations(self):
        state = _make_state()
        state.set_selected("BH-1", 0.0, True)
        # entity rows
        assert state.is_checked("BH-1", 0.0) is True
        # vertical rows (row=depth, col=site)
        state.row_dim = "vertical"
        state.col_dim = "entity"
        assert state.is_checked(0.0, "BH-1") is True


class TestOrientationSwap:
    def test_swap_preserves_checked(self):
        state = _make_state()
        state.set_selected("BH-1", 0.0, True)
        state.set_selected("BH-1", 1.0, True)
        state.set_selected("BH-2", 3.0, True)
        checked_before = set(state._checked)
        state.row_dim = "vertical"
        state.col_dim = "entity"
        assert state._checked == checked_before

    def test_row_keys_col_keys_swap(self):
        state = _make_state()
        state.row_dim = "entity"
        state.col_dim = "vertical"
        assert state.row_keys == ["BH-1", "BH-2"]
        assert state.col_keys == [0.0, 1.0, 2.0, 3.0]
        state.row_dim = "vertical"
        state.col_dim = "entity"
        assert state.row_keys == [0.0, 1.0, 2.0, 3.0]
        assert state.col_keys == ["BH-1", "BH-2"]


class TestGetSelectedCombinations:
    def test_returns_checked_only(self):
        state = _make_state()
        state.set_selected("BH-1", 0.0, False)
        state.set_selected("BH-2", 3.0, True)
        combos = state.get_selected_combinations()
        # BH-1's depth 0 unchecked, BH-1 depths 1,2 still checked by default
        # BH-2 all depths checked, BH-2 depth 3 checked by default
        expected = {
            (0, 1),  # BH-1 @ 1.0
            (0, 2),  # BH-1 @ 2.0
            (1, 0),  # BH-2 @ 1.0
            (1, 1),  # BH-2 @ 2.0
            (1, 2),  # BH-2 @ 3.0
        }
        assert set(combos) == expected


class TestSelectDeselectAll:
    def test_select_all(self):
        state = _make_state()
        state.set_selected("BH-1", 0.0, False)
        state.set_selected("BH-1", 1.0, False)
        state.select_all()
        assert state.is_checked("BH-1", 0.0) is True
        assert state.is_checked("BH-1", 1.0) is True

    def test_deselect_all(self):
        state = _make_state()
        state.deselect_all()
        assert len(state._checked) == 0


class TestClear:
    def test_clear_removes_all(self):
        state = _make_state()
        state.clear()
        assert len(state._sites) == 0
        assert len(state._checked) == 0


class TestVersionBumps:
    def test_add_site_bumps_version_and_layout(self):
        state = SelectionState()
        v, lv = state.version, state.layout_version
        state.add_site(0, "BH-1", [0.0], [[1]], ["2020-01-01"])
        assert state.version > v
        assert state.layout_version > lv

    def test_set_checked_bumps_version(self):
        state = _make_state()
        v = state.version
        state.set_checked("BH-1", 0.0, False)
        assert state.version > v

    def test_noop_set_checked_does_not_bump(self):
        """Setting checked to its current value should not bump version."""
        state = _make_state()
        v = state.version
        state.set_checked("BH-1", 0.0, True)  # already checked
        assert state.version == v
