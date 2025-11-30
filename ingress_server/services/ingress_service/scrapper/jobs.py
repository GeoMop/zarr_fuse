import logging
import requests

import polars as pl

from datetime import datetime

from packages.common import s3io, validation
from packages.common.models import ActiveScrapperConfig, MetadataModel

LOG = logging.getLogger("scrapper-jobs")

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

        content_type = response.headers.get("Content-Type", "")
        ok, err = validation.validate_content_type(content_type)
        if not ok:
            LOG.warning("Validation content type failed for %s: %s", content_type, err)
            return

        payload = response.content
        ok, err = validation.validate_data(payload)
        if not ok:
            LOG.warning("Validating data failed for %s", err)
            return None, f"Scrapper job {name} received invalid response from {url}: {err}"

        return response, None
    except Exception as e:
        return None, f"Scrapper job {name} failed to fetch {url}: {e}"

def process_data(scrapper_config: ActiveScrapperConfig, params_dict: dict, headers_dict: dict, row: dict | None = None) -> str | None:
    LOG.debug("Scrapper job %s processing with param %s", scrapper_config.name, params_dict)

    response, err = request_caller(
        name=scrapper_config.name,
        url=scrapper_config.url,
        headers=headers_dict,
        params=params_dict
    )
    if err:
        LOG.error(err)
        return err

    content_type = response.headers.get("Content-Type", "")
    meta_data = MetadataModel(
        content_type=content_type,
        endpoint_name=scrapper_config.name,
        username=f"scrapper-{scrapper_config.name}",
        schema_name=scrapper_config.schema_name,
        received_at=datetime.utcnow(),
        extract_fn=scrapper_config.extract_fn,
        fn_module=scrapper_config.fn_module,
        dataframe_row=row,
    )

    location = s3io.save_accepted_object(scrapper_config.name, "", content_type, response.content, meta_data)

    LOG.info("Scrapper accepted name=%s location=%s", scrapper_config.name, location)
    return None


def run_job(scrapper_config: ActiveScrapperConfig):
    headers_dict = {
        h.header_name: h.header_value
        for h in scrapper_config.headers or []
    }

    if scrapper_config.dataframe_path:
        try:
            df = pl.read_csv(scrapper_config.dataframe_path, has_header=scrapper_config.dataframe_has_header)
        except Exception as e:
            LOG.error("Scrapper job %s failed to read dataframe from %s: %s", scrapper_config.name, scrapper_config.dataframe_path, e)
            return

        for row in df.iter_rows(named=True):
            params_dict = {
                p.param_name: row.get(p.column_name)
                for p in scrapper_config.url_params or []
            }

            if None in params_dict.values():
                LOG.info("Scrapper job %s skipping row %s due to missing parameter values", scrapper_config.name, row)
                continue

            err = process_data(scrapper_config, params_dict=params_dict, headers_dict=headers_dict, row=row)
            if err:
                LOG.error("Scrapper job %s failed to save data for row %s: %s", scrapper_config.name, row, err)
                continue
    else:
        params_dict = {
            p.param_name: p.column_name
            for p in scrapper_config.url_params or []
        }
        err = process_data(scrapper_config, params_dict=params_dict, headers_dict=headers_dict)
        if err:
            LOG.error("Scrapper job %s failed to save data: %s", scrapper_config.name, err)
