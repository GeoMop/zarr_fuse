import logging

from apscheduler.schedulers.background import BackgroundScheduler

from ingress_service.scrapper import jobs
from packages.common.models.configuration_model import ScrapperConfig

LOG = logging.getLogger("scrapper-scheduler")

def add_scrapper_job(scheduler: BackgroundScheduler, scrapper: ScrapperConfig):
    minute, hour, day, month, dow = scrapper.cron.split()
    scheduler.add_job(
        jobs.run_job,
        "cron",
        args=[scrapper],
        minute=minute, hour=hour, day=day, month=month, day_of_week=dow,
        id=f"scrapper-{scrapper.name}", replace_existing=True,
        name=scrapper.name,
    )
