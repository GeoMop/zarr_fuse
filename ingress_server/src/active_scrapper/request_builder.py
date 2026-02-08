from typing import Dict, Tuple

from active_scrapper.context import ExecutionContext, ExecutionContextError
from active_scrapper.active_scrapper_config_models import (
    RequestConfig,
    ActiveScrapperHeader,
    QueryParamConfig,
)


def build_request(
    request_cfg: RequestConfig,
    ctx: ExecutionContext,
) -> Tuple[str, Dict[str, str], Dict[str, str]]:
    url = _render_string(request_cfg.url, ctx)

    headers = _build_headers(request_cfg.headers, ctx)
    params = _build_query_params(request_cfg.query_params, ctx)

    return url, headers, params


def _build_headers(headers_cfg: list[ActiveScrapperHeader], ctx: ExecutionContext) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for h in headers_cfg:
        name = h.header_name
        value = _render_string(h.header_value, ctx)
        out[name] = value
    return out


def _build_query_params(params_cfg: list[QueryParamConfig], ctx: ExecutionContext) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for p in params_cfg:
        name = p.name
        value = _render_string(p.value, ctx)
        out[name] = value
    return out


def _render_string(template: str, ctx: ExecutionContext) -> str:
    try:
        return ctx.render(template)
    except ExecutionContextError as e:
        raise ExecutionContextError(
            f"Failed to render template '{template}'. "
            f"Context={ctx.to_dict()}. Error: {e}"
        ) from e
