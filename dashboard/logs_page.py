"""Standalone ``/logs`` page that stays reachable when the dashboard fails.

This module provides the emergency log page and its incremental JSON feed. It
is registered through ``extra_patterns`` in :mod:`dashboard.serve_dashboard`
and depends only on :mod:`dashboard.log_buffer` and
:mod:`dashboard.log_format` — no view config, store access or Panel app
construction. That keeps it usable when ``build_dashboard`` raises during the
initial load.

``GET /logs`` returns the HTML page; ``GET /logs.json?since=N`` returns the lines
added after sequence ``N`` so the page can poll incrementally. Links opened from
the dashboard carry ``?from=dashboard`` and get a close-tab control instead of
the dashboard link.
"""

from __future__ import annotations

import json
from typing import Final

from tornado.web import RequestHandler

from dashboard import log_buffer
from dashboard.log_format import LOG_CSS, POLL_MS, SCROLL_JS, line_class

LOGS_TAB_URL: Final[str] = "/logs?from=dashboard"

_BACK_DASHBOARD: Final[str] = '<a href="/">&#8592; dashboard</a>'
_BACK_CLOSE: Final[str] = (
    '<button type="button" id="zf-close" title="Close this tab">close tab &times;</button>'
)

PAGE: Final[str] = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Server logs</title>
<style>
html, body { height: 100%; margin: 0; }
body {
    background: #0b1220;
    color: #cbd5e1;
    font-family: "Cascadia Mono", "Consolas", "Menlo", ui-monospace, monospace;
}
.zf-log-view { height: 100vh; }
#zf-status {
    padding: 4px 12px;
    border-top: 1px solid #1f2937;
    font-size: 11px;
    color: #64748b;
}
__LOG_CSS__
</style>
</head>
<body>
<div class="zf-log-view">
  <div class="zf-log-header"><span>SERVER LOGS</span>__BACK_HTML__</div>
  <div class="zf-log-scroll">
    <pre class="zf-log" id="zf-log-body"><span class="zf-empty">No output captured yet.</span></pre>
  </div>
  <div id="zf-status">connecting&#8230;</div>
</div>
<script>
__SCROLL_JS__
</script>
<script>
const POLL_MS = __POLL_MS__;
const MAX_LINES = __MAX_LINES__;
let since = 0;
const body = document.getElementById('zf-log-body');
const status = document.getElementById('zf-status');
const closeBtn = document.getElementById('zf-close');

if (closeBtn) {
  closeBtn.addEventListener('click', function () {
    window.close();
    setTimeout(function () {
      if (!window.closed) {
        closeBtn.textContent = 'close manually';
        closeBtn.title = 'The browser blocked automatic closing; please close this tab manually.';
      }
    }, 150);
  });
}

function addLine(ts, text, cls) {
  if (body.querySelector('.zf-empty')) body.textContent = '';
  const row = document.createElement('span');
  row.className = 'zf-line';
  const stamp = document.createElement('span');
  stamp.className = 'zf-ts';
  stamp.textContent = ts;
  const message = document.createElement('span');
  if (cls) message.className = cls;
  message.textContent = text;
  row.appendChild(stamp);
  row.appendChild(message);
  body.appendChild(row);
  while (body.childElementCount > MAX_LINES) {
    body.removeChild(body.firstElementChild);
  }
}

async function poll() {
  try {
    const response = await fetch('/logs.json?since=' + since, {cache: 'no-store'});
    if (!response.ok) throw new Error('HTTP ' + response.status);
    const payload = await response.json();
    if (payload.next < since) {
      since = 0;
      body.textContent = '';
      return;
    }
    for (const line of payload.lines) {
      addLine(line[0], line[1], line[2]);
    }
    since = payload.next;
    status.textContent = payload.count + ' lines buffered';
  } catch (err) {
    status.textContent = 'disconnected: ' + err.message + ' (retrying)';
  }
}

poll();
setInterval(poll, POLL_MS);
</script>
</body>
</html>
"""


def build_payload(since: int = 0) -> dict[str, object]:
    """Build the incremental JSON payload for the standalone page."""
    rows, next_seq = log_buffer.entries_since(since)
    return {
        "lines": [[stamp, text, line_class(text)] for _, stamp, text in rows],
        "next": next_seq,
        "count": log_buffer.count(),
    }


def render_page(*, from_dashboard: bool = False) -> str:
    """Render the page, using a close-tab control for dashboard-opened tabs."""
    back_html = _BACK_CLOSE if from_dashboard else _BACK_DASHBOARD
    return (
        PAGE.replace("__LOG_CSS__", LOG_CSS)
        .replace("__SCROLL_JS__", SCROLL_JS)
        .replace("__POLL_MS__", str(POLL_MS))
        .replace("__MAX_LINES__", str(log_buffer.MAX_LINES))
        .replace("__BACK_HTML__", back_html)
    )


class LogsJsonHandler(RequestHandler):
    """Incremental JSON feed for the standalone page."""

    def get(self) -> None:
        try:
            since = max(0, int(self.get_query_argument("since", "0")))
        except ValueError:
            since = 0
        self.set_header("Content-Type", "application/json; charset=utf-8")
        self.set_header("Cache-Control", "no-store")
        self.write(json.dumps(build_payload(since)))


class LogsPageHandler(RequestHandler):
    """Standalone HTML log page, independent of the dashboard app."""

    def get(self) -> None:
        self.set_header("Content-Type", "text/html; charset=utf-8")
        self.set_header("Cache-Control", "no-store")
        from_dashboard = self.get_query_argument("from", "") == "dashboard"
        self.write(render_page(from_dashboard=from_dashboard))


LOG_ROUTES = [
    (r"/logs", LogsPageHandler),
    (r"/logs\.json", LogsJsonHandler),
]
