import logging

from active_scrapper.active_scrapper_config_models import (
    ActiveScrapperConfig,
    RunConfig,
)
from active_scrapper.context import ExecutionContextError
from active_scrapper.planner import build_contexts_for_run
from active_scrapper.request_builder import build_request
from io_utils import validate_response, process_payload

import requests

LOG = logging.getLogger("active-scrapper")


def request_caller(
    name: str,
    url: str,
    headers: dict | None = None,
    params: dict | None = None,
) -> tuple[requests.Response | None, str | None]:
    try:
        response = requests.get(
            url=url,
            params=params,
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()

        err = validate_response(
            response.content,
            response.headers.get("Content-Type", ""),
        )
        if err:
            return None, f"Scrapper job {name} received invalid response from {url}: {err}"

        return response, None
    except Exception as e:
        return None, f"Scrapper job {name} failed to fetch {url}: {e}"


def run_one_scheduled_run(scrapper: ActiveScrapperConfig, run_cfg: RunConfig) -> None:
    try:
        contexts = build_contexts_for_run(scrapper, run_cfg)
    except Exception as e:
        LOG.error("Scrapper %s failed to build contexts for cron=%s: %s", scrapper.name, run_cfg.cron, e)
        return

    if not contexts:
        LOG.warning("Scrapper %s produced 0 contexts for cron=%s", scrapper.name, run_cfg.cron)
        return

    LOG.info(
        "Scrapper %s running cron=%s: %d request(s)",
        scrapper.name,
        run_cfg.cron,
        len(contexts),
    )

    headers_static = {h.header_name: h.header_value for h in scrapper.request.headers}

    for ctx in contexts:
        try:
            url, headers, params = build_request(scrapper.request, ctx)

            merged_headers = dict(headers_static)
            merged_headers.update(headers)

            resp, err = request_caller(
                name=scrapper.name,
                url=url,
                headers=merged_headers,
                params=params,
            )
            if err:
                LOG.error(err)
                continue

            success, perr = process_payload(
                data_source=scrapper.data_source,
                payload=resp.content,
                content_type=resp.headers.get("Content-Type"),
                username=f"scrapper-{scrapper.name}",
                dataframe_row=ctx.to_dict(),
            )
            if not success:
                LOG.error("Scrapper %s failed to save payload for ctx=%s: %s", scrapper.name, ctx.to_dict(), perr)

        except ExecutionContextError as e:
            LOG.error("Scrapper %s context/render error for ctx=%s: %s", scrapper.name, ctx.to_dict(), e)
        except Exception as e:
            LOG.exception("Scrapper %s unexpected error for ctx=%s: %s", scrapper.name, ctx.to_dict(), e)
