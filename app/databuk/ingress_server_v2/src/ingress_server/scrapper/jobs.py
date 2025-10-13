import logging
import requests

from common import s3io
from common.models.metadata_model import MetadataModel
from common.models.configuration_model import ScrapperConfig

LOG = logging.getLogger("scrapper-jobs")

def run_job(job: ScrapperConfig):
    response = requests.get(job.url, timeout=30)
    response.raise_for_status()
    payload = response.content
    content_type = response.headers.get("Content-Type", "application/json")

    meta_data = MetadataModel(
        content_type=content_type,
        endpoint_name=job.name,
        username=f"scrapper-{job.name}",
        schema_path=job.schema_path,
    )

    location = s3io.save_accepted_object(job.name, "", content_type, payload, meta_data)

    LOG.info("Scrapper accepted name=%s loc=%s", job.name, location)
