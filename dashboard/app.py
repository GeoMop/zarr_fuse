"""
HoloViz Dashboard Prototype

A Panel-based dashboard demonstrating:
- GoldenLayout for resizable panes
- Linked interactive visualizations (map + time-series)
- Multi-layer geographic maps with GeoViews
- Dashboard-style control panel
"""

from composed import build_dashboard

tmpl = build_dashboard()
tmpl.servable(title="HoloViz Prototypes")
