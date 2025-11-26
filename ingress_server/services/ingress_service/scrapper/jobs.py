import logging
import requests

from packages.common import s3io, validation
from packages.common.models import ScrapperConfig, MetadataModel

LOG = logging.getLogger("scrapper-jobs")

def _call_method(url: str, method: str) -> requests.Response:
    if method.upper() == "GET":
        return requests.get(url, timeout=30)
    elif method.upper() == "POST":
        return requests.post(url, timeout=30)
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")

def run_job(job: ScrapperConfig):
    response = _call_method(job.url, job.method)
    response.raise_for_status()

    content_type = response.headers.get("Content-Type", "application/json")

    ok, err = validation.validate_content_type(content_type)
    if not ok:
        LOG.warning("Validation content type failed for %s: %s", content_type, err)
        return

    payload = response.content
    ok, err = validation.validate_data(payload, content_type)
    if not ok:
        LOG.warning("Validating data failed for %s", err)
        return

    meta_data = MetadataModel(
        content_type=content_type,
        endpoint_name=job.name,
        username=f"scrapper-{job.name}",
        schema_name=job.schema_name,
    )

    location = s3io.save_accepted_object(job.name, "", content_type, payload, meta_data)

    LOG.info("Scrapper accepted name=%s loc=%s", job.name, location)
