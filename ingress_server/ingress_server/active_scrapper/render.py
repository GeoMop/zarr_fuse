import logging
from collections.abc import Iterable
from datetime import datetime, timezone

from .context import ExecutionContext, ExecutionContextError
from .active_scrapper_config_models import (
    RenderValue,
    RenderSource,
)

LOG = logging.getLogger(__name__)


def apply_render_values(
    ctx: ExecutionContext,
    render_values: Iterable[RenderValue],
) -> ExecutionContext:
    values = list(render_values)
    LOG.debug("Applying %d render values: %s", len(values), [v.name for v in values])

    current = ctx
    for rv in values:
        current = _apply_render_value(current, rv)
    return current


def datetime_utc(fmt: str) -> str:
    if not fmt:
        raise ExecutionContextError("Format string cannot be empty for datetime_utc render value")
    return datetime.now(timezone.utc).strftime(fmt)


def datetime_local(fmt: str) -> str:
    if not fmt:
        raise ExecutionContextError("Format string cannot be empty for datetime_local render value")
    return datetime.now().astimezone().strftime(fmt)


def const(value: str) -> str:
    return value


def _apply_render_value(ctx: ExecutionContext, rv: RenderValue) -> ExecutionContext:
    LOG.debug("Applying render value '%s' with source '%s'", rv.name, rv.source)
    if rv.name in ctx:
        raise ExecutionContextError(f"Render variable '{rv.name}' already exists in context")

    match rv.source:
        case RenderSource.CONST:
            value = const(rv.value)
        case RenderSource.DATETIME_UTC:
            value = datetime_utc(rv.format)
        case RenderSource.DATETIME_LOCAL:
            value = datetime_local(rv.format)
        case _:
            raise ExecutionContextError(f"Unsupported render source: {rv.source}")

    return ctx.with_value(rv.name, value)
