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

SHAPE_TO_SVG = {
    "circle":
        '<svg viewBox="0 0 16 16" width="14" height="14">'
        '<circle cx="8" cy="8" r="5" fill="currentColor"/></svg>',
    "square":
        '<svg viewBox="0 0 16 16" width="14" height="14">'
        '<rect x="3" y="3" width="10" height="10" fill="currentColor"/></svg>',
    "diamond":
        '<svg viewBox="0 0 16 16" width="14" height="14">'
        '<polygon points="8,1 15,8 8,15 1,8" fill="currentColor"/></svg>',
    "triangle":
        '<svg viewBox="0 0 16 16" width="14" height="14">'
        '<polygon points="8,1 15,14 1,14" fill="currentColor"/></svg>',
    "inverted_triangle":
        '<svg viewBox="0 0 16 16" width="14" height="14">'
        '<polygon points="8,15 15,2 1,2" fill="currentColor"/></svg>',
    "hex":
        '<svg viewBox="0 0 16 16" width="14" height="14">'
        '<polygon points="8,1 14,4 14,12 8,15 2,12 2,4" fill="currentColor"/></svg>',
    "star":
        '<svg viewBox="0 0 16 16" width="14" height="14">'
        '<polygon points="8,1 9.5,5.5 14.5,5.5 10.5,8.5 12,13 8,10 4,13 5.5,8.5 1.5,5.5 6.5,5.5" fill="currentColor"/></svg>',  # noqa: E501
    "cross":
        '<svg viewBox="0 0 16 16" width="14" height="14">'
        '<path d="M3,3 L13,13 M13,3 L3,13" stroke="currentColor" stroke-width="2"/>'
        '</svg>',
    "x":
        '<svg viewBox="0 0 16 16" width="14" height="14">'
        '<path d="M3,3 L13,13 M13,3 L3,13" stroke="currentColor" stroke-width="2"/>'
        '</svg>',
    "diamond_cross":
        '<svg viewBox="0 0 16 16" width="14" height="14">'
        '<polygon points="8,1 15,8 8,15 1,8" fill="currentColor" opacity="0.3"/>'
        '<path d="M8,3 L8,13 M3,8 L13,8" stroke="currentColor" stroke-width="1.5"/>'
        '</svg>',
    "circle_cross":
        '<svg viewBox="0 0 16 16" width="14" height="14">'
        '<circle cx="8" cy="8" r="5" fill="currentColor" opacity="0.3"/>'
        '<path d="M8,4 L8,12 M4,8 L12,8" stroke="currentColor" stroke-width="1.5"/>'
        '</svg>',
    "square_cross":
        '<svg viewBox="0 0 16 16" width="14" height="14">'
        '<rect x="3" y="3" width="10" height="10" fill="currentColor" opacity="0.3"/>'
        '<path d="M5,5 L11,11 M11,5 L5,11" stroke="currentColor" stroke-width="1.5"/>'
        '</svg>',
    "triangle_cross":
        '<svg viewBox="0 0 16 16" width="14" height="14">'
        '<polygon points="8,1 15,14 1,14" fill="currentColor" opacity="0.3"/>'
        '<path d="M8,4 L8,12 M4,10 L12,10" stroke="currentColor" stroke-width="1.5"/>'
        '</svg>',
}
