from .context import ExecutionContext, ExecutionContextError
from .active_scrapper_config_models import (
    RequestConfig,
    ActiveScrapperHeader,
    QueryParamConfig,
)


def build_request(
    ctx: ExecutionContext,
    request_cfg: RequestConfig,
) -> tuple[str, dict[str, str], dict[str, str]]:
    url = _render_string(request_cfg.url, ctx)

    headers = _build_headers(request_cfg.headers, ctx)
    params = _build_query_params(request_cfg.query_params, ctx)

    return url, headers, params


def _build_headers(headers_cfg: list[ActiveScrapperHeader], ctx: ExecutionContext) -> dict[str, str]:
    return {
        h.header_name: _render_string(h.header_value, ctx)
        for h in headers_cfg
    }


def _build_query_params(params_cfg: list[QueryParamConfig], ctx: ExecutionContext) -> dict[str, str]:
    return {
        h.name: _render_string(h.value, ctx)
        for h in params_cfg
    }


def _render_string(template: str, ctx: ExecutionContext) -> str:
    try:
        return template.format(**ctx.values)
    except KeyError as e:
        raise ExecutionContextError(
            f"Failed to render template '{template}'. "
            f"Missing context variable: {e.args[0]}. "
            f"Context={ctx.to_dict()}"
        ) from e
