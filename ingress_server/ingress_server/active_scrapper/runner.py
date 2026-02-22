import requests
import logging

from pathlib import Path
from urllib.parse import urlparse

from .active_scrapper_config_models import (
    ActiveScrapperConfig,
    RunConfig,
)
from .context import ExecutionContextError
from .planner import build_contexts_for_run
from .request_builder import build_request
from ..io import process_payload

LOG = logging.getLogger("active-scrapper.runner")


def effective_content_type(http_ct: str | None, url: str) -> str:
    ct = (http_ct or "").lower()
    name = Path(urlparse(url).path).name.lower()

    if name.endswith((".grb.bz2", ".grib.bz2", ".grb2.bz2", ".grib2.bz2")):
        return "application/x-grib+bz2"
    if name.endswith((".grb", ".grib", ".grb2", ".grib2")):
        return "application/x-grib"
    return ct

def request_caller(
    name: str,
    url: str,
    headers: dict | None = None,
    params: dict | None = None,
) -> requests.Response:
    try:
        response = requests.get(
            url=url,
            params=params,
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()

        return response
    except Exception as e:
        raise ValueError(f"Scrapper job {name} failed to fetch {url}: {e}")


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
            url, headers, params = build_request(ctx, scrapper.request)

            merged_headers = dict(headers_static)
            merged_headers.update(headers)

            resp = request_caller(
                name=scrapper.name,
                url=url,
                headers=merged_headers,
                params=params,
            )

            success, perr = process_payload(
                data_source=scrapper.data_source,
                payload=resp.content,
                content_type=effective_content_type(resp.headers.get("Content-Type"), url),
                username=f"scrapper-{scrapper.name}",
                dataframe_row=ctx.to_dict(),
            )
            if not success:
                LOG.error("Scrapper %s failed to save payload for ctx=%s: %s", scrapper.name, ctx.to_dict(), perr)

        except ExecutionContextError as e:
            LOG.error("Scrapper %s context/render error for ctx=%s: %s", scrapper.name, ctx.to_dict(), e)
        except Exception as e:
            LOG.exception("Scrapper %s unexpected error for ctx=%s: %s", scrapper.name, ctx.to_dict(), e)
