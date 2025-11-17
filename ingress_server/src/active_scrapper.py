import logging
import requests
import logging

import polars as pl

from io_utils import validate_response, save_data
from models import ActiveScrapperConfig

from apscheduler.schedulers.background import BackgroundScheduler

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
            timeout=30
        )
        response.raise_for_status()

        err = validate_response(
            response.content,
            response.headers.get("Content-Type", "")
        )
        if err:
            return None, f"Scrapper job {name} received invalid response from {url}: {err}"

        return response, None
    except Exception as e:
        return None, f"Scrapper job {name} failed to fetch {url}: {e}"

def process_job_with_dataframe(scrapper_config: ActiveScrapperConfig):
    try:
        df = pl.read_csv(scrapper_config.dataframe_path, has_header=scrapper_config.dataframe_has_header)
    except Exception as e:
        LOG.error(
            "Scrapper job %s failed to read dataframe from %s: %s",
            scrapper_config.name,
            scrapper_config.dataframe_path,
            e,
        )
        return

    headers_dict = {
        h.header_name: h.header_value
        for h in scrapper_config.headers or []
    }

    for row in df.iter_rows(named=True):
        params_dict = {
            p.param_name: row.get(p.column_name)
            for p in scrapper_config.url_params or []
        }

        if None in params_dict.values():
            LOG.info(
                "Scrapper job %s skipping row %s due to missing parameter values",
                scrapper_config.name,
                row,
            )
            continue

        LOG.debug("Scrapper job %s processing with param %s", scrapper_config.name, params_dict)

        response, err = request_caller(
            name=scrapper_config.name,
            url=scrapper_config.url,
            headers=headers_dict,
            params=params_dict
        )
        if err:
            LOG.error(err)
            continue

        err = save_data(
            name=scrapper_config.name,
            payload=response.content,
            content_type=response.headers.get("Content-Type", "application/json"),
            schema_path=scrapper_config.schema_path,
            extract_fn=scrapper_config.extract_fn,
            fn_module=scrapper_config.fn_module,
            dataframe_row=row,
            username=f"scrapper-{scrapper_config.name}",
        )
        if err:
            LOG.error("Scrapper job %s failed to save data for row %s: %s", scrapper_config.name, row, err)
            continue


def process_job_without_dataframe(scrapper_config: ActiveScrapperConfig):
    headers_dict = {
        h.header_name: h.header_value
        for h in scrapper_config.headers or []
    }
    params_dict = {
        p.param_name: p.column_name
        for p in scrapper_config.url_params or []
    }

    response, err = request_caller(
        name=scrapper_config.name,
        url=scrapper_config.url,
        headers=headers_dict,
        params=params_dict
    )
    if err:
        LOG.error(err)
        return

    err = save_data(
        name=scrapper_config.name,
        payload=response.content,
        content_type=response.headers.get("Content-Type", "application/json"),
        schema_path=scrapper_config.schema_path,
        extract_fn=scrapper_config.extract_fn,
        fn_module=scrapper_config.fn_module,
        username=f"scrapper-{scrapper_config.name}",
    )
    if err:
        LOG.error("Scrapper job %s failed to save data: %s", scrapper_config.name, err)
        return

    LOG.info("Scrapper accepted name=%s", scrapper_config.name)


def run_job(scrapper_config: ActiveScrapperConfig):
    if scrapper_config.dataframe_path:
        process_job_with_dataframe(scrapper_config)
    else:
        process_job_without_dataframe(scrapper_config)

def add_scrapper_job(scrapper_config: ActiveScrapperConfig, scheduler: BackgroundScheduler):
    cron_job_id = 1
    for cron in scrapper_config.crons:
        minute, hour, day, month, dow = cron.split()
        scheduler.add_job(
            run_job,
            "cron",
            args=[scrapper_config],
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=dow,
            id=f"scrapper-{scrapper_config.name}-{cron_job_id}",
            replace_existing=True,
            name=scrapper_config.name,
        )
        cron_job_id += 1
