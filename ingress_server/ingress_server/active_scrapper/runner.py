import time
import requests
import logging

from pathlib import Path
from urllib.parse import urlparse

from ..io import process_payload, send_failure_email
from ..app_config import AppConfig
from .context import ExecutionContext, ExecutionContextError
from .iterate import expand_iterate
from .render import apply_render_values
from .request_builder import build_request
from .active_scrapper_config_models import (
    ActiveScrapperConfig,
    RunConfig,
)

LOG = logging.getLogger(__name__)


def _effective_content_type(http_ct: str | None, url: str) -> str:
    ct = (http_ct or "").lower()
    name = Path(urlparse(url).path).name.lower()

    if name.endswith((".grb.bz2", ".grib.bz2", ".grb2.bz2", ".grib2.bz2")):
        return "application/x-grib+bz2"
    if name.endswith((".grb", ".grib", ".grb2", ".grib2")):
        return "application/x-grib"
    return ct


def _request_caller(
    name: str,
    url: str,
    headers: dict | None = None,
    params: dict | None = None,
    max_attempts: int = 5,
) -> requests.Response:
    last_error = None

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(
                url=url,
                params=params,
                headers=headers,
                timeout=30,
            )

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    sleep_s = int(retry_after)
                else:
                    sleep_s = min(2 ** (attempt - 1), 30)

                LOG.warning(
                    "Scrapper %s got 429 for %s, retrying in %ss (attempt %s/%s)",
                    name,
                    url,
                    sleep_s,
                    attempt,
                    max_attempts,
                )
                time.sleep(sleep_s)
                continue

            response.raise_for_status()
            return response

        except requests.RequestException as exc:
            last_error = exc
            if attempt == max_attempts:
                break

            sleep_s = min(2 ** (attempt - 1), 30)
            LOG.warning(
                "Scrapper %s request failed for %s: %s. Retrying in %ss (attempt %s/%s)",
                name,
                url,
                exc,
                sleep_s,
                attempt,
                max_attempts,
            )
            time.sleep(sleep_s)

    raise ValueError(f"Scrapper job {name} failed to fetch {url}: {last_error}")


def _build_contexts_for_run(
    app_config: AppConfig,
    scrapper_config: ActiveScrapperConfig,
    run_cfg: RunConfig,
) -> list[ExecutionContext]:
    initial_ctx = ExecutionContext(dict(run_cfg.set))
    rendered_ctx = apply_render_values(initial_ctx, scrapper_config.render)

    contexts: list[ExecutionContext] = [rendered_ctx]

    for it in scrapper_config.iterate:
        next_contexts: list[ExecutionContext] = []
        for ctx in contexts:
            try:
                next_contexts.extend(
                    expand_iterate(app_config, ctx, it, scrapper_config.data_source)
                )
            except Exception as exc:
                raise ExecutionContextError(
                    f"Failed to expand iterator '{getattr(it, 'name', '<unknown>')}' "
                    f"for scrapper '{scrapper_config.name}': {exc}"
                ) from exc

        contexts = next_contexts

        if not contexts:
            LOG.warning(
                "Iterator '%s' produced 0 contexts for scrapper '%s' (run cron=%s).",
                getattr(it, "name", "<unknown>"),
                scrapper_config.name,
                run_cfg.cron,
            )
            break

    return contexts


def run_one_scheduled_run(
    app_config: AppConfig,
    scrapper: ActiveScrapperConfig,
    run_cfg: RunConfig,
) -> None:
    failures: list[dict] = []

    try:
        contexts = _build_contexts_for_run(app_config, scrapper, run_cfg)
    except ExecutionContextError as exc:
        LOG.error(
            "Scrapper %s failed to build contexts for cron=%s: %s",
            scrapper.name,
            run_cfg.cron,
            exc,
        )
        failures.append({
            "type": "build_contexts",
            "context": {},
            "error": str(exc),
        })
        contexts = []
    except Exception as exc:
        LOG.exception(
            "Scrapper %s unexpectedly failed to build contexts for cron=%s",
            scrapper.name,
            run_cfg.cron,
        )
        failures.append({
            "type": "build_contexts_unexpected",
            "context": {},
            "error": str(exc),
        })
        contexts = []

    LOG.info(
        "Scrapper %s running cron=%s: %d request(s)",
        scrapper.name,
        run_cfg.cron,
        len(contexts),
    )

    headers_static = {h.header_name: h.header_value for h in scrapper.request.headers}

    for ctx in contexts:
        ctx_dict = ctx.to_dict()

        try:
            url, headers, params = build_request(ctx, scrapper.request)

            merged_headers = dict(headers_static)
            merged_headers.update(headers)

            resp = _request_caller(
                name=scrapper.name,
                url=url,
                headers=merged_headers,
                params=params,
            )

            process_payload(
                app_config=app_config,
                data_source=scrapper.data_source,
                payload=resp.content,
                content_type=_effective_content_type(resp.headers.get("Content-Type"), url),
                username=f"scrapper-{scrapper.name}",
                dataframe_row=ctx_dict,
            )

        except ExecutionContextError as exc:
            LOG.error(
                "Scrapper %s context/render error for ctx=%s: %s",
                scrapper.name,
                ctx_dict,
                exc,
            )
            failures.append({
                "type": "context_render",
                "context": ctx_dict,
                "error": str(exc),
            })

        except ValueError as exc:
            LOG.warning(
                "Scrapper %s rejected payload for ctx=%s: %s",
                scrapper.name,
                ctx_dict,
                exc,
            )
            failures.append({
                "type": "payload_validation",
                "context": ctx_dict,
                "error": str(exc),
            })

        except Exception as exc:
            LOG.exception(
                "Scrapper %s unexpected error for ctx=%s",
                scrapper.name,
                ctx_dict,
            )
            failures.append({
                "type": "unexpected",
                "context": ctx_dict,
                "error": str(exc),
            })

    if failures:
        try:
            send_failure_email(
                scrapper_name=scrapper.name,
                cron=run_cfg.cron,
                failures=failures,
            )
        except Exception:
            LOG.exception(
                "Scrapper %s failed to send failure notification email",
                scrapper.name,
            )
