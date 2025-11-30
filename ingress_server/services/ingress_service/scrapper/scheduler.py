from apscheduler.schedulers.background import BackgroundScheduler

from . import jobs
from packages.common.models import ActiveScrapperConfig

def add_scrapper_job(scheduler: BackgroundScheduler, scrapper: ActiveScrapperConfig):
    index = 0
    for cron in scrapper.crons:
        minute, hour, day, month, dow = cron.split()
        scheduler.add_job(
            jobs.run_job,
            "cron",
            args=[scrapper],
            minute=minute, hour=hour, day=day, month=month, day_of_week=dow,
            id=f"scrapper-{scrapper.name}-{index}", replace_existing=True,
            name=scrapper.name,
        )
        index += 1
