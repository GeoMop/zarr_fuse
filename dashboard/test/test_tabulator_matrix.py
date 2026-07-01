"""Unit tests for build_assignment_matrix — DataFrame, editors, styles."""

from __future__ import annotations

import numpy as np
import pytest

from dashboard.plot_selection import SelectionState, build_assignment_matrix


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
        # 1 label col (_row_label) + 2 internal + 3 depth + 3 hidden valid = 9
        assert df.shape[1] == 9

    def test_vertical_rows_entity_cols(self):
        state = _two_site_state()
        df, _, _, _, _, _ = build_assignment_matrix(state, "vertical", "entity")
        assert len(df) == 4  # header row + 3 unique depths
        # 1 label col (_row_label) + 2 internal + 2 site + 2 hidden valid = 7
        assert df.shape[1] == 7


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
        state.set_selected("BH-1", 0.0, False)
        state.set_selected("BH-2", 2.0, True)

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
