"""Shared plot style constants for the dashboard."""

COLORS = [
    "#e6194b", "#3cb44b", "#4363d8", "#f58231", "#911eb4",
    "#42d4f4", "#f032e6", "#bfef45", "#fabed4", "#469990",
    "#dcbeff", "#9a6324", "#800000", "#aaffc3", "#808000",
    "#ffd8b1", "#000075", "#a9a9a9", "#e6beff", "#1a1a1a",
]

LINE_DASHES = ["solid", "dashed", "dotted", "dashdot", "longdash", "dashdotdot"]

_DASH_CSS = {
    "solid": "solid",
    "dashed": "dashed",
    "dotted": "dotted",
    "dashdot": "dashed",
    "longdash": "dashed",
    "dashdotdot": "dashdot",
}

MARKER_SHAPES = [
    "circle", "square", "diamond", "triangle", "inverted_triangle",
    "hex", "star", "cross", "x", "diamond_cross",
    "circle_cross", "square_cross", "triangle_cross",
]

SHAPE_TO_DASH = {
    "circle": "solid",
    "square": "dashed",
    "diamond": "dotted",
    "triangle": "dashdot",
    "inverted_triangle": "dotdash",
    "hex": "dashdot",
    "star": "dotdash",
    "cross": "solid",
    "x": "dashed",
    "diamond_cross": "dotted",
    "circle_cross": "dashdot",
    "square_cross": "dotdash",
    "triangle_cross": "dashdot",
}
