import logging
from typing import Iterable
from datetime import datetime, timezone

from active_scrapper.context import ExecutionContext, ExecutionContextError
from active_scrapper.active_scrapper_config_models import (
    RenderValue,
    RenderSource,
)

LOG = logging.getLogger("active-scrapper")

def apply_render_values(
    ctx: ExecutionContext,
    render_values: Iterable[RenderValue],
) -> ExecutionContext:
    LOG.debug("Applying render values: %s", list(render_values))
    out = ctx
    for rv in render_values:
        out = apply_render_value(out, rv)
    return out

def datetime_utc(format: str) -> str:
    LOG.debug("Rendering datetime_utc with format '%s'", format)
    return datetime.now(timezone.utc).strftime(format)

def datetime_local(format: str) -> str:
    LOG.debug("Rendering datetime_local with format '%s'", format)
    return datetime.now().astimezone().strftime(format)

def const(value: str) -> str:
    LOG.debug("Rendering const with value '%s'", value)
    return value

def apply_render_value(ctx: ExecutionContext, rv: RenderValue) -> ExecutionContext:
    LOG.debug("Applying render value '%s' with source '%s'", rv.name, rv.source)
    if rv.name in ctx:
        raise ExecutionContextError(f"Render variable '{rv.name}' already exists in context")

    if rv.source == RenderSource.CONST:
        return ctx.with_value(rv.name, rv.value)

    if rv.source == RenderSource.DATETIME_UTC:
        if not rv.format:
            raise ExecutionContextError("Render source datetime_utc requires 'format'")
        value = datetime.now(timezone.utc).strftime(rv.format)
        return ctx.with_value(rv.name, value)

    if rv.source == RenderSource.DATETIME_LOCAL:
        if not rv.format:
            raise ExecutionContextError("Render source datetime_local requires 'format'")
        value = datetime.now().astimezone().strftime(rv.format)
        return ctx.with_value(rv.name, value)

    raise ExecutionContextError(f"Unsupported render source: {rv.source}")
