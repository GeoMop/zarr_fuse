"""Shared formatting for the dashboard log surfaces.

This module defines the colour rules, CSS, and auto-scroll script used by both
the in-app modal (:mod:`dashboard.log_view`) and the standalone fallback page
(:mod:`dashboard.logs_page`), so the two views cannot drift apart.
"""

from __future__ import annotations

import html
import re
from typing import Final

_ERROR = re.compile(
    r"Traceback|\b\w*(?:Error|Exception)\b|\b(?:ERROR|CRITICAL|FATAL|FAILED)\b",
    re.IGNORECASE,
)
_WARNING = re.compile(r"\bWARN(?:ING)?\b", re.IGNORECASE)
_PERF = re.compile(r"\[perf\]")
_POPULATE = re.compile(r"\[(?:populate|session|reload)\]")
_ACTIVITY = re.compile(
    r"\[(?:fetch_ts|fill_ts|tap|SelectionState|plot_selection)\]"
)
_DEBUG = re.compile(r"\[(?:config|ylim|timeseries)\]")

POLL_MS: Final[int] = 1000

LOG_CSS: Final[str] = """
.zf-log-view {
    display: flex;
    flex-direction: column;
    min-height: 0;
    overflow: hidden;
    background: #0b1220;
}
.zf-log-scroll {
    flex: 1 1 auto;
    min-height: 0;
    min-width: 0;
    overflow-y: auto;
    overflow-x: hidden;
}
.zf-log-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 6px 10px;
    border-bottom: 1px solid #1f2937;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.04em;
    color: #94a3b8;
}
.zf-log-header a {
    color: #93c5fd;
    font-size: 11px;
    text-decoration: none;
    margin-right: 30px;
}
.zf-log-header a:hover { text-decoration: underline; }
.zf-log {
    margin: 0;
    padding: 8px 10px 8px 10px;
    font-family: "Cascadia Mono", "Consolas", "Menlo", ui-monospace, monospace;
    font-size: 12px;
    line-height: 1.45;
    color: #cbd5e1;
    background: #0b1220;
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    word-break: break-word;
    tab-size: 4;
    box-sizing: border-box;
    width: 100%;
    max-width: 100%;
}
.zf-log .zf-line {
    display: block;
    width: 100%;
}
.zf-log .zf-ts { color: #475569; margin-right: 8px; }
.zf-log .zf-empty { color: #64748b; font-style: italic; }
.zf-err { color: #f87171; }
.zf-warn { color: #fbbf24; }
.zf-perf { color: #60a5fa; }
.zf-pop { color: #c084fc; }
.zf-act { color: #2dd4bf; }
.zf-dbg { color: #94a3b8; }
"""

SCROLL_JS: Final[str] = """
(function(){
  function find(root, selector){
    const direct = root.querySelector(selector);
    if(direct){ return direct; }
    for(const el of root.querySelectorAll('*')){
      if(el.shadowRoot){
        const found = find(el.shadowRoot, selector);
        if(found){ return found; }
      }
    }
    return null;
  }
  function init(){
    const root = find(document, '.zf-log-view');
    if(!root){ setTimeout(init, 60); return; }
    const scroller = find(root.shadowRoot || root, '.zf-log-scroll');
    if(!scroller){ setTimeout(init, 60); return; }
    const body = find(scroller.shadowRoot || scroller, '.zf-log-body');
    if(body && !body.shadowRoot){ setTimeout(init, 60); return; }
    const target = body ? body.shadowRoot : (scroller.shadowRoot || scroller);
    if(!target){ setTimeout(init, 60); return; }
    if(scroller.__zfLogBound){ return; }
    scroller.__zfLogBound = true;
    let pinned = true;
    const updatePinned = function(){
      pinned = (scroller.scrollTop + scroller.clientHeight >= scroller.scrollHeight - 8);
    };
    scroller.addEventListener('scroll', updatePinned, {passive:true});
    const obs = new MutationObserver(function(){
      if(pinned){
        scroller.scrollTop = scroller.scrollHeight;
      }
    });
    obs.observe(target, {childList:true, subtree:true, characterData:true});
    setTimeout(function(){ scroller.scrollTop = scroller.scrollHeight; pinned = true; }, 50);
  }
  if(document.readyState === 'loading'){
    document.addEventListener('DOMContentLoaded', init, {once:true});
  } else {
    init();
  }
})();
"""

_CLASSES: Final[tuple[tuple[re.Pattern[str], str], ...]] = (
    (_ERROR, "zf-err"),
    (_WARNING, "zf-warn"),
    (_PERF, "zf-perf"),
    (_POPULATE, "zf-pop"),
    (_ACTIVITY, "zf-act"),
    (_DEBUG, "zf-dbg"),
)


def line_class(text: str) -> str:
    """Return the CSS class that colours one log line."""
    for pattern, css_class in _CLASSES:
        if pattern.search(text):
            return css_class
    return ""


def render_body(rows: list[tuple[str, str]]) -> str:
    """Render ``(timestamp, line)`` pairs into the escaped body markup."""
    if not rows:
        return (
            '<pre class="zf-log" id="zf-log-body">'
            '<span class="zf-empty">No output captured yet.</span></pre>'
        )
    parts = ['<pre class="zf-log" id="zf-log-body">']
    for stamp, text in rows:
        css_class = line_class(text)
        class_attr = f' class="{css_class}"' if css_class else ""
        parts.append(
            f'<span class="zf-line"><span class="zf-ts">{html.escape(stamp)}</span>'
            f"<span{class_attr}>{html.escape(text)}</span></span>"
        )
    parts.append("</pre>")
    return "".join(parts)
