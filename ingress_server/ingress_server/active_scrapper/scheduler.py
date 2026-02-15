import logging

from apscheduler.schedulers.background import BackgroundScheduler

from .runner import run_one_scheduled_run
from .active_scrapper_config_models import (
    ActiveScrapperConfig,
    RunConfig,
)

LOG = logging.getLogger("active-scrapper")


def add_scrapper_jobs(scrapper: ActiveScrapperConfig, scheduler: BackgroundScheduler) -> None:
    if not scrapper.runs:
        LOG.warning("Scrapper %s has no runs configured - no jobs registered", scrapper.name)
        return

    for idx, run_cfg in enumerate(scrapper.runs, start=1):
        _add_one_job(scheduler, scrapper, run_cfg, idx)

    LOG.info("Registered %d job(s) for scrapper %s", len(scrapper.runs), scrapper.name)


def _add_one_job(
    scheduler: BackgroundScheduler,
    scrapper: ActiveScrapperConfig,
    run_cfg: RunConfig,
    idx: int,
) -> None:
    cron = run_cfg.cron
    minute, hour, day, month, dow = cron.split()

    job_id = f"scrapper-{scrapper.name}-{idx}"

    scheduler.add_job(
        run_one_scheduled_run,
        trigger="cron",
        args=[scrapper, run_cfg],
        minute=minute,
        hour=hour,
        day=day,
        month=month,
        day_of_week=dow,
        id=job_id,
        replace_existing=True,
        name=f"{scrapper.name} [{cron}]",
    )

    LOG.info(
        "Added job %s for scrapper=%s cron=%s set=%s",
        job_id,
        scrapper.name,
        cron,
        getattr(run_cfg, "set", {}),
    )
