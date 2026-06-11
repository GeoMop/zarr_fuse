"""Unit tests for dynamic row shape / column color mapping."""

from __future__ import annotations

import numpy as np
import pytest

from dashboard.plot_selection import (
    SelectionState,
    _build_legend_html,
    build_assignment_matrix,
)
from dashboard.plot_styles import COLORS, MARKER_SHAPES


def _one_site_one_depth():
    state = SelectionState()
    state.add_site(0, "BH-1", [5.0], [[1]], ["2020-01-01"])
    return state


def _n_sites(count: int, depth: float = 0.0):
    state = SelectionState()
    for i in range(count):
        state.add_site(i, f"BH-{i}", [depth], [[i + 1]], ["2020-01-01"])
    return state


def _one_site_n_depths(count: int):
    depths = list(range(count))
    data = [[i + j for j in range(count)] for i in range(count)]
    state = SelectionState()
    state.add_site(0, "BH-1", depths, data, ["2020-01-01"] * count)
    return state


class TestRowShapesCycle:
    def test_fewer_rows_than_shapes(self):
        state = _one_site_one_depth()
        _, _, _, _, rshapes, _ = build_assignment_matrix(state, "entity", "vertical")
        assert rshapes == {"BH-1": "circle"}

    def test_shapes_cycle(self):
        state = _n_sites(len(MARKER_SHAPES) + 1)
        _, _, _, _, rshapes, _ = build_assignment_matrix(state, "entity", "vertical")
        assert len(rshapes) == len(MARKER_SHAPES) + 1
        # First row → "circle", last row (index 13) → "circle" again (cycles)
        assert rshapes["BH-0"] == "circle"
        assert rshapes[f"BH-{len(MARKER_SHAPES)}"] == "circle"  # cycles back


class TestColColorsCycle:
    def test_fewer_cols_than_colors(self):
        state = _one_site_one_depth()
        _, _, _, _, _, ccolors = build_assignment_matrix(state, "entity", "vertical")
        assert ccolors == {"5.0": "#e6194b"}

    def test_colors_cycle(self):
        state = _one_site_n_depths(len(COLORS) + 1)
        _, _, _, _, _, ccolors = build_assignment_matrix(state, "entity", "vertical")
        assert len(ccolors) == len(COLORS) + 1
        keys = sorted(ccolors.keys(), key=float)
        assert ccolors[keys[0]] == COLORS[0]
        assert ccolors[keys[-1]] == COLORS[0]  # cycles back


class TestStyleMapsSwap:
    def test_vertical_rows_entity_cols(self):
        """Shapes → depths, colors → sites."""
        state = _n_sites(3)
        _, _, _, _, rshapes, ccolors = build_assignment_matrix(state, "vertical", "entity")
        assert set(rshapes.keys()) == {"0.0"}          # a single depth
        assert set(ccolors.keys()) == {"BH-0", "BH-1", "BH-2"}
        assert rshapes["0.0"] == "circle"
        assert ccolors["BH-0"] == COLORS[0]
        assert ccolors["BH-1"] == COLORS[1]

    def test_state_stores_swapped_maps(self):
        state = _n_sites(2, 1.0)
        build_assignment_matrix(state, "vertical", "entity")
        assert set(state._row_shapes.keys()) == {"1.0"}
        assert set(state._col_colors.keys()) == {"BH-0", "BH-1"}


class TestLegendHTML:
    def test_legend_empty_no_sites(self):
        state = SelectionState()
        html = _build_legend_html(state)
        assert "No curves selected" in html and "<i>" in html

    def test_legend_shows_dynamic_shapes_colors(self):
        state = _n_sites(2, 5.0)
        build_assignment_matrix(state, "entity", "vertical")
        html = _build_legend_html(state)
        assert "circle" in html or "square" in html
        assert "#e6194b" in html or "#3cb44b" in html
        assert "Color" in html
        assert "Shape" in html

    def test_legend_headers_reflect_orientation(self):
        """When rows = entity and cols = vertical, legend labels should
        say 'Shape — Site' and 'Color — Depth'."""
        state = _n_sites(2, 5.0)
        build_assignment_matrix(state, "entity", "vertical")
        html = _build_legend_html(state)
        assert "Shape — Site:" in html
        assert "Color — Depth:" in html

    def test_legend_swapped_headers(self):
        """When rows = vertical and cols = entity, legend labels should
        say 'Shape — Depth' and 'Color — Site'."""
        state = _n_sites(2, 5.0)
        state.row_dim = "vertical"
        state.col_dim = "entity"
        build_assignment_matrix(state, "vertical", "entity")
        html = _build_legend_html(state)
        assert "Shape — Depth:" in html
        assert "Color — Site:" in html
