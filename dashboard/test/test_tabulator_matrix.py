"""Unit tests for build_assignment_matrix — DataFrame, editors, styles."""

from __future__ import annotations

import time as _time
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import panel as pn
import pytest

from dashboard.plot_selection import SelectionState, build_assignment_matrix, build_plot_selection_panel


def _two_site_state():
    state = SelectionState()
    state.add_site(0, "BH-1", [0.0, 1.0], [[1, 2], [3, 4]], ["2020-01-01", "2020-01-02"])
    state.add_site(1, "BH-2", [1.0, 2.0], [[5, 6], [7, 8]], ["2020-01-01", "2020-01-02"])
    return state


class TestMatrixShape:
    def test_entity_rows_vertical_cols(self):
        state = _two_site_state()
        df, editors, formatters, editables, rshapes, ccolors = build_assignment_matrix(
            state, "entity", "vertical"
        )
        assert len(df) == 3  # header row + 2 sites
        # 1 label col (_row_label) + 1 raw key (_row_key) + 2 internal + 3 depth + 3 hidden valid = 10
        assert df.shape[1] == 10

    def test_vertical_rows_entity_cols(self):
        state = _two_site_state()
        df, _, _, _, _, _ = build_assignment_matrix(state, "vertical", "entity")
        assert len(df) == 4  # header row + 3 unique depths
        # 1 label col (_row_label) + 1 raw key (_row_key) + 2 internal + 2 site + 2 hidden valid = 8
        assert df.shape[1] == 8


class TestDataFrameContent:
    def test_row_labels(self):
        state = _two_site_state()
        df, _, _, _, _, _ = build_assignment_matrix(state, "entity", "vertical")
        assert df["_row_label"].iloc[0] == "All"
        assert str(df["_row_label"].iloc[1]).endswith("BH-1")
        assert str(df["_row_label"].iloc[2]).endswith("BH-2")

    def test_row_labels_swapped(self):
        state = _two_site_state()
        df, _, _, _, _, _ = build_assignment_matrix(state, "vertical", "entity")
        assert df["_row_label"].iloc[0] == "All"
        assert str(df["_row_label"].iloc[1]).endswith("0.0")
        assert str(df["_row_label"].iloc[2]).endswith("1.0")
        assert str(df["_row_label"].iloc[3]).endswith("2.0")

    def test_hidden_validity_columns(self):
        state = _two_site_state()
        df, _, _, _, _, _ = build_assignment_matrix(state, "entity", "vertical")
        assert "__valid_0.0" in df.columns
        assert "__valid_1.0" in df.columns
        assert "__valid_2.0" in df.columns

    def test_invalid_cell_empty_string(self):
        """BH-1 has depths [0.0, 1.0]; 2.0 should be empty."""
        state = _two_site_state()
        df, _, _, _, _, _ = build_assignment_matrix(state, "entity", "vertical")
        bh1 = df[df["_row_label"].str.endswith("BH-1")].iloc[0]
        assert bh1["2.0"] == ""
        assert bool(bh1["__valid_2.0"]) is False
        assert bh1["0.0"] == "✓"

    def test_valid_cell_display(self):
        state = _two_site_state()
        df, _, _, _, _, _ = build_assignment_matrix(state, "entity", "vertical")
        bh2 = df[df["_row_label"].str.endswith("BH-2")].iloc[0]
        assert bh2["1.0"] == "✓"
        assert bool(bh2["__valid_1.0"]) is True


class TestEditors:
    def test_label_marker_actions_editors_none(self):
        state = _two_site_state()
        _, editors, _, _, _, _ = build_assignment_matrix(state, "entity", "vertical")
        assert editors["_row_label"] is None
        assert editors["_actions"] is None

    def test_boolean_editors_none(self):
        state = _two_site_state()
        _, editors, _, _, _, _ = build_assignment_matrix(state, "entity", "vertical")
        for col in ["0.0", "1.0", "2.0"]:
            assert editors[col] is None


class TestFormatters:
    def test_label_formatters_text(self):
        state = _two_site_state()
        _, _, formatters, _, _, _ = build_assignment_matrix(state, "entity", "vertical")
        assert formatters["_row_label"]["type"] == "html"
        assert formatters["_actions"]["type"] == "button"
        assert formatters["_actions"]["label"] == "✕ Remove"
        assert formatters["_actions"]["buttonType"] == "danger"

    def test_boolean_formatters_html(self):
        state = _two_site_state()
        _, _, formatters, _, _, _ = build_assignment_matrix(state, "entity", "vertical")
        for col in ["0.0", "1.0", "2.0"]:
            assert formatters[col]["type"] == "html"


class TestEditables:
    def test_labels_not_editable(self):
        state = _two_site_state()
        _, _, _, editables, _, _ = build_assignment_matrix(state, "entity", "vertical")
        assert editables["_row_label"] is False

    def test_boolean_cols_not_editable(self):
        state = _two_site_state()
        _, _, _, editables, _, _ = build_assignment_matrix(state, "entity", "vertical")
        for col in ["0.0", "1.0", "2.0"]:
            assert editables[col] is False


class TestStyleMaps:
    def test_row_shapes_assigned(self):
        state = _two_site_state()
        _, _, _, _, rshapes, _ = build_assignment_matrix(state, "entity", "vertical")
        assert set(rshapes.keys()) == {"BH-1", "BH-2"}

    def test_col_colors_assigned(self):
        state = _two_site_state()
        _, _, _, _, _, ccolors = build_assignment_matrix(state, "entity", "vertical")
        assert set(ccolors.keys()) == {"0.0", "1.0", "2.0"}

    def test_style_maps_on_state(self):
        state = _two_site_state()
        build_assignment_matrix(state, "entity", "vertical")
        assert state._row_shapes == {"BH-1": "circle", "BH-2": "square"}
        assert state._col_colors == {"0.0": "#e6194b", "1.0": "#3cb44b", "2.0": "#4363d8"}


class TestOrientationSwapPreservesSelection:
    def test_checked_state_survives_swap(self):
        state = _two_site_state()
        state.set_checked("BH-1", 0.0, False)
        state.set_checked("BH-2", 2.0, True)

        df1, _, _, _, _, _ = build_assignment_matrix(state, "entity", "vertical")
        assert df1[df1["_row_label"].str.endswith("BH-1")].iloc[0]["0.0"] == "✗"
        assert df1[df1["_row_label"].str.endswith("BH-2")].iloc[0]["2.0"] == "✓"

        df2, _, _, _, _, _ = build_assignment_matrix(state, "vertical", "entity")
        depth0_row = df2[df2["_row_label"].str.endswith("0.0")].iloc[0]
        # BH-1 at depth 0.0 was unchecked
        assert depth0_row["BH-1"] == "✗"

        depth2_row = df2[df2["_row_label"].str.endswith("2.0")].iloc[0]
        # BH-2 at depth 2.0 was checked
        assert depth2_row["BH-2"] == "✓"


class TestStateOrientationRestored:
    def test_state_unchanged_after_call(self):
        state = _two_site_state()
        state.row_dim = "entity"
        state.col_dim = "vertical"
        build_assignment_matrix(state, "vertical", "entity")
        assert state.row_dim == "entity"
        assert state.col_dim == "vertical"

    def test_style_maps_on_state_match_requested_dims(self):
        state = _two_site_state()
        build_assignment_matrix(state, "vertical", "entity")
        # Style maps should match the passed dims, not the restored state
        assert set(state._row_shapes.keys()) == {"0.0", "1.0", "2.0"}
        assert set(state._col_colors.keys()) == {"BH-1", "BH-2"}


class TestEdgeCases:
    def test_empty_state(self):
        state = SelectionState()
        df, editors, formatters, editables, rshapes, ccolors = build_assignment_matrix(
            state, "entity", "vertical"
        )
        assert len(df) == 1  # just the header row
        assert df.iloc[0]["_row_label"] == "All"
        assert len(editors) == 2  # _row_label, _actions
        assert len(rshapes) == 0
        assert len(ccolors) == 0

    def test_single_site(self):
        state = SelectionState()
        state.add_site(0, "BH-1", [5.0], [[1]], ["2020-01-01"])
        df, _, _, _, _, _ = build_assignment_matrix(state, "entity", "vertical")
        assert len(df) == 2  # header row + 1 site
        assert str(df.iloc[1]["_row_label"]).endswith("BH-1")

    def test_entity_index_column_present(self):
        state = SelectionState()
        state.add_site(0, "BH-1", [5.0], [[1]], ["2020-01-01"])
        state.add_site(1, "BH-2", [5.0], [[1]], ["2020-01-01"])
        df, _, _, _, _, _ = build_assignment_matrix(state, "entity", "vertical")
        assert "entity_index" in df.columns
        assert list(df["entity_index"])[1:] == [0.0, 1.0]

    def test_entity_index_nan_in_vertical_mode(self):
        state = SelectionState()
        state.add_site(0, "BH-1", [5.0], [[1]], ["2020-01-01"])
        df, _, _, _, _, _ = build_assignment_matrix(state, "vertical", "entity")
        assert "entity_index" in df.columns
        # header row has NaN, data row also has NaN in vertical mode
        assert all(np.isnan(v) for v in df["entity_index"])


class TestIndividualCellPatch:
    """Phase 1: individual cell toggle must patch, not rebuild."""

    def _make_tabulator(self):
        state = _two_site_state()
        df, editors, formatters, editables, _, _ = build_assignment_matrix(
            state, "entity", "vertical"
        )
        hidden = [c for c in df.columns if c.startswith("__valid_") or c in ("entity_index", "_row_key")]
        table = pn.widgets.Tabulator(
            df,
            editors=editors,
            formatters=formatters,
            hidden_columns=hidden,
            selectable=False,
            show_index=False,
            sizing_mode="stretch_width",
            layout="fit_data_table",
            sortable=False,
        )
        return table, state

    def test_patch_called_instead_of_rebuild(self):
        """One cell toggle must call table.patch with as_index=False, not table.value setter."""
        table, state = self._make_tabulator()
        patch_calls = []
        patch_kwargs_list = []
        original_patch = table.patch

        def mock_patch(patch_value, **kwargs):
            patch_calls.append(patch_value)
            patch_kwargs_list.append(kwargs)
            original_patch(patch_value, **kwargs)

        table.patch = mock_patch

        df_before = table.value

        row_idx = 1
        col = "0.0"
        row_data = table.value.iloc[row_idx]
        assert row_data["_row_key"] == "BH-1"

        row_key = row_data["_row_key"]
        current = state.is_checked(row_key, col)
        assert current is True
        new_checked = not current
        state.set_checked(row_key, col, new_checked, bump_version=False)
        new_value = "\u2713" if new_checked else "\u2717"
        table.patch({col: [(row_idx, new_value)]}, as_index=False)

        assert state.is_checked("BH-1", 0.0) is False
        assert len(patch_calls) == 1
        assert "0.0" in patch_calls[0]
        assert table.value is df_before
        assert table.value.iloc[row_idx]["0.0"] == "\u2717"
        assert patch_kwargs_list[0].get("as_index") is False

    def test_as_index_false_uses_positional_row(self):
        """With as_index=False the row index is positional, not DataFrame index."""
        table, state = self._make_tabulator()
        row_idx = 1
        col = "0.0"
        state.set_checked("BH-1", 0.0, False, bump_version=False)
        table.patch({col: [(row_idx, "\u2717")]}, as_index=False)
        assert table.value.iloc[row_idx][col] == "\u2717"

    def test_toggle_back_restores_original(self):
        """Toggle twice restores the original checked state."""
        table, state = self._make_tabulator()
        row_idx = 1
        col = "0.0"

        state.set_checked("BH-1", 0.0, False, bump_version=False)
        table.patch({col: [(row_idx, "\u2717")]}, as_index=False)
        assert table.value.iloc[row_idx][col] == "\u2717"

        state.set_checked("BH-1", 0.0, True, bump_version=False)
        table.patch({col: [(row_idx, "\u2713")]}, as_index=False)
        assert table.value.iloc[row_idx][col] == "\u2713"
        assert state.is_checked("BH-1", 0.0) is True

    def test_invalid_cell_not_patched(self):
        """Invalid cells (empty string) must not be patched."""
        table, state = self._make_tabulator()
        row_idx = 1
        col = "2.0"
        row_data = table.value.iloc[row_idx]
        assert row_data["_row_key"] == "BH-1"
        assert row_data.get(f"__valid_{col}", False) == False
        assert row_data[col] == ""

    def test_patch_count_and_state_in_sync(self):
        """After patching, _checked_count matches actual checked cells."""
        table, state = self._make_tabulator()
        initial_count = state._checked_count

        state.set_checked("BH-1", 0.0, False, bump_version=False)
        table.patch({"0.0": [(1, "\u2717")]}, as_index=False)
        assert state._checked_count == initial_count - 1

        state.set_checked("BH-1", 0.0, True, bump_version=False)
        table.patch({"0.0": [(1, "\u2713")]}, as_index=False)
        assert state._checked_count == initial_count


class TestPatchFailureRecovery:
    """Phase 1: patch failure must roll back state and clear loading."""

    def _make_tabulator_and_loading(self):
        state = _two_site_state()
        df, editors, formatters, editables, _, _ = build_assignment_matrix(
            state, "entity", "vertical"
        )
        hidden = [c for c in df.columns if c.startswith("__valid_") or c in ("entity_index", "_row_key")]
        table = pn.widgets.Tabulator(
            df,
            editors=editors,
            formatters=formatters,
            hidden_columns=hidden,
            selectable=False,
            show_index=False,
            sizing_mode="stretch_width",
            layout="fit_data_table",
            sortable=False,
        )
        loading = pn.Row(visible=True)
        return table, state, loading

    def test_patch_failure_rolls_back_state(self):
        """If table.patch raises, the canonical state must revert."""
        table, state, loading = self._make_tabulator_and_loading()

        row_idx = 1
        col = "0.0"
        row_key = "BH-1"
        current = state.is_checked(row_key, col)
        assert current is True

        state.set_checked(row_key, col, False, bump_version=False)

        def bad_patch(*args, **kwargs):
            raise RuntimeError("patch failed")

        table.patch = bad_patch

        try:
            new_value = "\u2717"
            try:
                table.patch({col: [(row_idx, new_value)]}, as_index=False)
            except Exception:
                state.set_checked(row_key, col, current, bump_version=False)
                if loading is not None:
                    loading.visible = False
                raise
        except RuntimeError:
            pass

        assert state.is_checked(row_key, col) is True
        assert ("BH-1", 0.0) in state._checked

    def test_loading_cleared_on_patch_failure(self):
        """Loading indicator must be hidden after patch failure."""
        table, state, loading = self._make_tabulator_and_loading()
        assert loading.visible is True

        def bad_patch(*args, **kwargs):
            raise RuntimeError("patch failed")

        table.patch = bad_patch

        state.set_checked("BH-1", 0.0, False, bump_version=False)
        try:
            try:
                table.patch({"0.0": [(1, "\u2717")]}, as_index=False)
            except Exception:
                state.set_checked("BH-1", 0.0, True, bump_version=False)
                loading.visible = False
                raise
        except RuntimeError:
            pass

        assert loading.visible is False

    def test_no_version_bump_on_patch_failure(self):
        """Version must not increment when patch fails and state rolls back."""
        table, state, loading = self._make_tabulator_and_loading()
        v_before = state.version

        def bad_patch(*args, **kwargs):
            raise RuntimeError("patch failed")

        table.patch = bad_patch

        state.set_checked("BH-1", 0.0, False, bump_version=False)
        try:
            try:
                table.patch({"0.0": [(1, "\u2717")]}, as_index=False)
            except Exception:
                state.set_checked("BH-1", 0.0, True, bump_version=False)
                loading.visible = False
                raise
        except RuntimeError:
            pass

        assert state.version == v_before

    def test_loading_none_handled_on_patch_failure(self):
        """When loading is None, patch failure must not raise AttributeError."""
        table, state, _ = self._make_tabulator_and_loading()

        def bad_patch(*args, **kwargs):
            raise RuntimeError("patch failed")

        table.patch = bad_patch

        state.set_checked("BH-1", 0.0, False, bump_version=False)
        exc_caught = False
        try:
            try:
                table.patch({"0.0": [(1, "\u2717")]}, as_index=False)
            except Exception:
                state.set_checked("BH-1", 0.0, True, bump_version=False)
                exc_caught = True
                raise
        except RuntimeError:
            exc_caught = True

        assert exc_caught
        assert state.is_checked("BH-1", 0.0) is True


_MS = 50  # debounce delay for tests (ms)
_WAIT = 0.12  # sleep to let debounce fire (must be > _MS/1000)


def _make_panel(debounce_ms=_MS):
    state = _two_site_state()
    loading = pn.Row(visible=False)
    panel, state = build_plot_selection_panel(
        state=state, table_loading=loading, _bump_delay_ms=debounce_ms,
    )
    return panel, state, loading


class TestTrailingDebounce:
    """Phase 2: version bumps must be debounced with a trailing timer."""

    def test_one_click_one_delayed_bump(self):
        """A single schedule_bump must not bump version immediately, only after delay."""
        _, state, _ = _make_panel()
        v_before = state.version
        state._panel_schedule_bump()
        assert state.version == v_before, "version must not change immediately"
        _time.sleep(_WAIT)
        assert state.version == v_before + 1, "version must increment after debounce"

    def test_five_rapid_clicks_one_final_bump(self):
        """Five rapid schedule_bump calls must produce exactly one version increment."""
        _, state, _ = _make_panel()
        v_before = state.version
        for _ in range(5):
            state._panel_schedule_bump()
            _time.sleep(0.005)
        assert state.version == v_before, "no bump during rapid clicks"
        _time.sleep(_WAIT)
        assert state.version == v_before + 1, "exactly one bump after debounce"

    def test_final_state_preserves_all_changes(self):
        """Multiple mutations before debounce fires must all be reflected in state."""
        _, state, _ = _make_panel()
        state.set_checked("BH-1", 0.0, False, bump_version=False)
        state._panel_schedule_bump()
        _time.sleep(0.01)
        state.set_checked("BH-2", 1.0, False, bump_version=False)
        state._panel_schedule_bump()
        _time.sleep(_WAIT)
        assert state.is_checked("BH-1", 0.0) is False
        assert state.is_checked("BH-2", 1.0) is False
        assert state.version >= 2

    def test_stale_timer_does_nothing(self):
        """Cancelling a pending timer must prevent the version bump."""
        _, state, _ = _make_panel()
        v_before = state.version
        state._panel_schedule_bump()
        state._panel_cancel_bump()
        _time.sleep(_WAIT)
        assert state.version == v_before, "cancelled timer must not bump"

    def test_later_click_produces_another_bump(self):
        """After debounce settles, a new click must produce a second bump."""
        _, state, _ = _make_panel()
        v_before = state.version
        state._panel_schedule_bump()
        _time.sleep(_WAIT)
        assert state.version == v_before + 1
        state._panel_schedule_bump()
        _time.sleep(_WAIT)
        assert state.version == v_before + 2

    def test_patch_failure_produces_no_bump(self):
        """A patch failure must not schedule a version bump."""
        _, state, loading = _make_panel()
        v_before = state.version
        loading.visible = True
        try:
            raise RuntimeError("patch failed")
        except RuntimeError:
            loading.visible = False
        _time.sleep(_WAIT)
        assert state.version == v_before

    def test_loading_cleared_after_debounce(self):
        """Loading indicator must be hidden when flush_bump fires."""
        _, state, loading = _make_panel()
        loading.visible = True
        state._panel_schedule_bump()
        assert loading.visible is True
        _time.sleep(_WAIT)
        assert loading.visible is False

    def test_loading_cleared_on_cancel_and_flush(self):
        """Loading indicator must be hidden when timer is cancelled and flushed."""
        _, state, loading = _make_panel()
        loading.visible = True
        state._panel_schedule_bump()
        state._panel_cancel_bump()
        state._panel_flush_bump()
        assert loading.visible is False

    def test_row_toggle_debounced(self):
        """Row toggle (set_all_for_row + schedule_bump) must be debounced."""
        _, state, _ = _make_panel()
        v_before = state.version
        state.set_checked("BH-1", 0.0, False, bump_version=False)
        state.set_checked("BH-1", 1.0, False, bump_version=False)
        state._panel_schedule_bump()
        assert state.version == v_before
        _time.sleep(_WAIT)
        assert state.is_checked("BH-1", 0.0) is False
        assert state.is_checked("BH-1", 1.0) is False
        assert state.version == v_before + 1

    def test_column_toggle_debounced(self):
        """Column toggle (set_all_for_column + schedule_bump) must be debounced."""
        _, state, _ = _make_panel()
        v_before = state.version
        state.set_all_for_column(1.0, False, bump_version=False)
        state._panel_schedule_bump()
        assert state.version == v_before
        _time.sleep(_WAIT)
        assert state.is_checked("BH-1", 1.0) is False
        assert state.is_checked("BH-2", 1.0) is False
        assert state.version == v_before + 1

    def test_select_all_debounced(self):
        """select_all + schedule_bump must be debounced."""
        _, state, _ = _make_panel()
        state.deselect_all(bump_version=False)
        _time.sleep(_WAIT)
        v_before = state.version
        state.select_all(bump_version=False)
        state._panel_schedule_bump()
        assert state.version == v_before
        _time.sleep(_WAIT)
        assert state._checked_count == state._valid_count
        assert state.version == v_before + 1

    def test_remove_site_debounced(self):
        """remove_site + schedule_bump must be debounced."""
        _, state, _ = _make_panel()
        v_before = state.version
        state.remove_site(1, bump_version=False)
        state._panel_schedule_bump()
        assert state.version == v_before
        _time.sleep(_WAIT)
        assert len(state.sites) == 1
        assert state.version == v_before + 1

    def test_orientation_bypasses_debounce(self):
        """Orientation changes bump version directly (not through debounce)."""
        _, state, _ = _make_panel()
        v_before = state.version
        state.row_dim = "vertical"
        state.col_dim = "entity"
        state.version += 1
        assert state.version == v_before + 1

    def test_flush_bump_resets_timer(self):
        """Calling flush_bump directly must clear the timer handle."""
        _, state, _ = _make_panel()
        state._panel_schedule_bump()
        assert state._panel_flush_bump is not None
        state._panel_flush_bump()
        assert state.version >= 2

    def test_cancel_bump_idempotent(self):
        """Cancelling when no timer is pending must not raise."""
        _, state, _ = _make_panel()
        state._panel_cancel_bump()
        state._panel_cancel_bump()
        v_before = state.version
        _time.sleep(_WAIT)
        assert state.version == v_before

    def test_rapid_clicks_with_state_changes(self):
        """Five rapid cell toggles must all be reflected and produce one bump."""
        _, state, _ = _make_panel()
        v_before = state.version
        pairs = [
            ("BH-1", 0.0, False),
            ("BH-1", 1.0, False),
            ("BH-2", 1.0, False),
            ("BH-2", 2.0, False),
            ("BH-1", 0.0, True),
        ]
        for rk, ck, val in pairs:
            state.set_checked(rk, ck, val, bump_version=False)
            state._panel_schedule_bump()
            _time.sleep(0.005)
        assert state.version == v_before
        _time.sleep(_WAIT)
        assert state.version == v_before + 1
        assert state.is_checked("BH-1", 0.0) is True
        assert state.is_checked("BH-1", 1.0) is False
        assert state.is_checked("BH-2", 1.0) is False
        assert state.is_checked("BH-2", 2.0) is False

    def test_bump_delay_ms_configurable(self):
        """A very short debounce delay must fire faster."""
        _, state, _ = _make_panel(debounce_ms=5)
        v_before = state.version
        state._panel_schedule_bump()
        _time.sleep(0.03)
        assert state.version == v_before + 1

    # ── Phase 2, requirement 2: stale callback guard ─────────────

    def test_stale_callback_does_not_bump_version(self):
        """Invoking _flush_bump with an old generation must not bump version."""
        _, state, _ = _make_panel(debounce_ms=5)
        gen_before = state._panel_bump_generation()
        state._panel_schedule_bump()
        gen_after_first = state._panel_bump_generation()
        assert gen_after_first == gen_before + 1
        state._panel_schedule_bump()
        gen_after_second = state._panel_bump_generation()
        assert gen_after_second == gen_before + 2
        v_before = state.version
        state._panel_flush_bump(generation=gen_after_first)
        assert state.version == v_before, "stale generation must not bump"

    def test_stale_callback_does_not_clear_newer_timer(self):
        """Invoking _flush_bump with an old generation must not touch _bump_timer."""
        _, state, _ = _make_panel()
        state._panel_schedule_bump()
        gen_old = state._panel_bump_generation()
        _time.sleep(0.005)
        state._panel_schedule_bump()
        gen_new = state._panel_bump_generation()
        assert gen_new > gen_old
        state._panel_flush_bump(generation=gen_old)
        _time.sleep(_WAIT)
        assert state.version >= 2, "newer timer must still fire"

    def test_stale_callback_does_not_hide_loading(self):
        """Invoking _flush_bump with an old generation must not clear loading."""
        _, state, loading = _make_panel()
        loading.visible = True
        state._panel_schedule_bump()
        gen_old = state._panel_bump_generation()
        state._panel_schedule_bump()
        gen_new = state._panel_bump_generation()
        assert gen_new > gen_old
        state._panel_flush_bump(generation=gen_old)
        assert loading.visible is True, "stale flush must not hide loading"
        _time.sleep(_WAIT)
        assert loading.visible is False

    # ── Phase 2, requirement 3: cancel before direct mutations ───

    def test_pending_click_then_orientation_change(self):
        """Orientation change must cancel pending debounce and bump only once."""
        _, state, loading = _make_panel()
        loading.visible = True
        state._panel_schedule_bump()
        gen = state._panel_bump_generation()
        state._panel_cancel_bump(clear_loading=True)
        state.row_dim = "vertical"
        state.col_dim = "entity"
        state.version += 1
        v_after_orientation = state.version
        _time.sleep(_WAIT)
        assert state.version == v_after_orientation, \
            "no extra bump from stale debounce after orientation"

    def test_pending_click_then_state_clear(self):
        """state.clear() must cancel pending debounce and not produce extra bump."""
        _, state, loading = _make_panel()
        state._panel_schedule_bump()
        _time.sleep(0.005)
        v_before_clear = state.version
        state.clear()
        v_after_clear = state.version
        _time.sleep(_WAIT)
        assert state.version == v_after_clear, \
            "no extra bump from stale debounce after clear"
        assert v_after_clear == v_before_clear + 1

    def test_orientation_clears_loading(self):
        """Orientation change must clear loading when it cancels pending debounce."""
        _, state, loading = _make_panel()
        loading.visible = True
        state._panel_schedule_bump()
        state._panel_cancel_bump(clear_loading=True)
        assert loading.visible is False

    # ── Phase 2, requirement 5: safe cancellation ────────────────

    def test_cancel_after_callback_already_ran(self):
        """Cancelling after the timer has already fired must not raise."""
        _, state, _ = _make_panel(debounce_ms=5)
        state._panel_schedule_bump()
        _time.sleep(0.03)
        state._panel_cancel_bump()
        v = state.version
        assert v >= 2

    def test_cancel_after_callback_ran_no_double_bump(self):
        """After timer fires, cancelling must not cause a second bump."""
        _, state, _ = _make_panel(debounce_ms=5)
        v_before = state.version
        state._panel_schedule_bump()
        _time.sleep(0.03)
        state._panel_cancel_bump()
        _time.sleep(_WAIT)
        assert state.version == v_before + 1

    # ── Phase 2, requirement 6: generation guard ─────────────────

    def test_generation_increments_on_each_schedule(self):
        """Each _schedule_bump call must increment the generation counter."""
        _, state, _ = _make_panel()
        g0 = state._panel_bump_generation()
        state._panel_schedule_bump()
        g1 = state._panel_bump_generation()
        state._panel_schedule_bump()
        g2 = state._panel_bump_generation()
        assert g1 == g0 + 1
        assert g2 == g0 + 2

    def test_flush_with_current_generation_bumps(self):
        """_flush_bump with the current generation must bump version."""
        _, state, _ = _make_panel()
        state._panel_schedule_bump()
        gen = state._panel_bump_generation()
        v_before = state.version
        state._panel_flush_bump(generation=gen)
        assert state.version == v_before + 1

    def test_flush_with_zero_generation_bumps(self):
        """_flush_bump(generation=0) must bump when no schedule has happened."""
        _, state, _ = _make_panel()
        v_before = state.version
        state._panel_flush_bump(generation=0)
        assert state.version == v_before + 1

    def test_no_doc_scheduler_uses_threading_timer(self):
        """In no-doc mode, schedule_bump must use threading.Timer (test-only)."""
        _, state, _ = _make_panel()
        state._panel_schedule_bump()
        import threading
        from dashboard.plot_selection import perf
        perf.reset()
        v_before = state.version
        _time.sleep(_WAIT)
        assert state.version == v_before + 1

    def test_add_site_force_cancels_pending_bump(self):
        """add_site(force=True) must cancel any pending debounce timer."""
        _, state, _ = _make_panel()
        state._panel_schedule_bump()
        gen_before = state._panel_bump_generation()
        state.add_site(0, "BH-1", [5.0], [[1]], ["2020-01-01"], force=True)
        gen_after = state._panel_bump_generation()
        _time.sleep(_WAIT)
        assert state.version >= 2

    def test_clear_cancels_pending_bump(self):
        """SelectionState.clear() must cancel any pending debounce timer."""
        _, state, _ = _make_panel()
        state._panel_schedule_bump()
        v_before = state.version
        state.clear()
        _time.sleep(_WAIT)
        assert state.version == v_before + 1

    def test_new_add_site_invalidates_pending_debounce(self):
        """add_site (new site, not force) must cancel pending debounce and bump only once."""
        _, state, _ = _make_panel()
        state._panel_schedule_bump()
        v_before = state.version
        _time.sleep(0.005)
        state.add_site(2, "BH-3", [0.0], [[1]], ["2020-01-01"])
        v_after_add = state.version
        assert v_after_add == v_before + 1, "add_site should produce exactly one version bump"
        _time.sleep(_WAIT)
        assert state.version == v_after_add, "pending debounce must not fire after add_site"

    def test_stale_callback_after_new_add_site(self):
        """After add_site cancels debounce, stale callback must not bump, corrupt timer, or change loading."""
        _, state, loading = _make_panel()
        loading.visible = True
        state._panel_schedule_bump()
        gen_old = state._panel_bump_generation()
        _time.sleep(0.005)
        state.add_site(2, "BH-3", [0.0], [[1]], ["2020-01-01"])
        gen_at_add = state._panel_bump_generation()
        v_after_add = state.version
        state._panel_flush_bump(generation=gen_old)
        assert state.version == v_after_add, "stale callback must not bump version"
        assert loading.visible is True, "stale callback must not change loading"
        state._panel_schedule_bump()
        _time.sleep(_WAIT)
        assert state.version >= v_after_add + 1, "new timer after add_site must still work"

    def test_version_bump_centralized_through_helper(self):
        """All SelectionState.version increments must go through _bump_version."""
        import inspect
        from dashboard.plot_selection import SelectionState
        source = inspect.getsource(SelectionState)
        lines = [l.strip() for l in source.splitlines() if "self.version += 1" in l]
        assert len(lines) == 1, (
            f"Expected exactly 1 self.version += 1 (in _bump_version), found {len(lines)}: {lines}"
        )
        assert "_bump_version" in source
