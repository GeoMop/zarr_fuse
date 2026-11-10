"""Panel component that renders the dashboard's captured terminal output.

This module provides the sidebar "Server logs" link and the in-app modal. The
modal shows a plain tail of :mod:`dashboard.log_buffer`: last lines only,
monospace, colourised by prefix, auto-scrolled to the newest line. It never
polls while closed.
"""

from __future__ import annotations

from typing import Final

import panel as pn

from dashboard import log_buffer
from dashboard.log_format import LOG_CSS, POLL_MS, SCROLL_JS, render_body
from dashboard.logs_page import LOGS_TAB_URL

RENDER_LIMIT: Final[int] = 300

_LINK_CSS = """
:host(.zf-logs-link) .bk-btn {
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
    color: #93c5fd !important;
    font-size: 12px !important;
    font-weight: 500 !important;
    padding: 2px 4px !important;
    text-align: left !important;
    text-transform: none !important;
}
:host(.zf-logs-link) .bk-btn:hover {
    background: transparent !important;
    color: #bfdbfe !important;
    text-decoration: underline;
}
"""

_LABEL_HTML = (
    '<div class="zf-log-header"><span>SERVER LOGS</span>'
    f'<a href="{LOGS_TAB_URL}" target="_blank" rel="noopener" '
    'title="Open full server logs in a new tab">open ↗</a></div>'
)


class LogView(pn.Column):
    """Plain auto-scrolling tail of the buffered terminal output."""

    def __init__(self, *, active: bool = True) -> None:
        self._callback = None

        self.label = pn.pane.HTML(
            _LABEL_HTML,
            margin=0,
            sizing_mode="stretch_width",
            stylesheets=[LOG_CSS],
        )
        self.body = pn.pane.HTML(
            render_body([]),
            css_classes=["zf-log-body"],
            sizing_mode="stretch_width",
            stylesheets=[LOG_CSS],
            styles={"background": "#0b1220"},
        )
        self.body_container = pn.Column(
            self.body,
            css_classes=["zf-log-scroll"],
            sizing_mode="stretch_both",
            styles={
                "background": "#0b1220",
                "overflow-y": "auto",
                "overflow-x": "hidden",
                "min-height": "0",
                "flex": "1 1 auto",
                "overflow-wrap": "break-word",
                "word-break": "break-word",
                "min-width": "0",
            },
        )

        self._scroller = pn.pane.HTML(
            f"<script>{SCROLL_JS}</script>",
            sizing_mode="stretch_width",
            height=0,
            margin=0,
        )

        super().__init__(
            self.label,
            self.body_container,
            self._scroller,
            css_classes=["zf-log-view"],
            sizing_mode="stretch_both",
            styles={
                "background": "#0b1220",
                "overflow": "hidden",
                "min-height": "0",
            },
        )

        if active and pn.state.curdoc is not None:
            self._callback = pn.state.add_periodic_callback(self.refresh, POLL_MS)

        self.refresh()

    def set_active(self, active: bool) -> None:
        """Start or stop polling; refresh immediately when enabling."""
        if active:
            if self._callback is None and pn.state.curdoc is not None:
                self._callback = pn.state.add_periodic_callback(self.refresh, POLL_MS)
            self.refresh()
        elif self._callback is not None:
            self._callback.stop()
            self._callback = None

    def refresh(self) -> None:
        """Re-render the newest lines from the buffer."""
        self.body.object = render_body(log_buffer.lines(RENDER_LIMIT))


def build_logs_link() -> pn.widgets.Button:
    """Sidebar-bottom text link that opens the log modal."""
    return pn.widgets.Button(
        label="Server logs",
        width=320,
        height=28,
        css_classes=["zf-logs-link"],
        stylesheets=[_LINK_CSS],
        margin=(8, 0, 0, 0),
    )
