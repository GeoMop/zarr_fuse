import logging

from apscheduler.schedulers.background import BackgroundScheduler

from ingress_service.scrapper import jobs
from common.models.configuration_model import ScrapperConfig

LOG = logging.getLogger("scrapper-scheduler")

def start_scrapper_jobs(scheduler: BackgroundScheduler, scrappers: list[ScrapperConfig]):
    if not scrappers:
        LOG.info("No active scrapper jobs configured")
        return

    for job in scrappers:
        minute, hour, day, month, dow = job.cron.split()
        scheduler.add_job(
            jobs.run_job,
            "cron",
            args=[job],
            minute=minute, hour=hour, day=day, month=month, day_of_week=dow,
            id=f"scrapper-{job.name}", replace_existing=True,
            name=job.name,
        )
