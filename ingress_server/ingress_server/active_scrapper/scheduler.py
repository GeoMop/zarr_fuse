import logging

from apscheduler.schedulers.background import BackgroundScheduler

from ..app_config import AppConfig
from .runner import run_one_scheduled_run
from .active_scrapper_config_models import (
    ActiveScrapperConfig,
    RunConfig,
)

LOG = logging.getLogger(__name__)


def add_scrapper_jobs(
    app_config: AppConfig,
    scrapper: ActiveScrapperConfig,
    scheduler: BackgroundScheduler,
) -> None:
    if not scrapper.runs:
        LOG.warning("Scrapper %s has no runs configured - no jobs registered", scrapper.name)
        return

    for run_idx, run_cfg in enumerate(scrapper.runs, start=1):
        _add_one_job(app_config, scheduler, scrapper, run_cfg, run_idx)

    LOG.info("Registered %d job(s) for scrapper %s", len(scrapper.runs), scrapper.name)


def _add_one_job(
    app_config: AppConfig,
    scheduler: BackgroundScheduler,
    scrapper: ActiveScrapperConfig,
    run_cfg: RunConfig,
    run_idx: int,
) -> None:
    cron = run_cfg.cron
    try:
        minute, hour, day, month, dow = cron.split()
    except ValueError as exc:
        raise ValueError(f"Invalid cron expression for scrapper {scrapper.name}: {cron}") from exc


    job_id = f"scrapper-{scrapper.name}-{run_idx}"

    scheduler.add_job(
        run_one_scheduled_run,
        trigger="cron",
        args=[app_config, scrapper, run_cfg],
        minute=minute,
        hour=hour,
        day=day,
        month=month,
        day_of_week=dow,
        id=job_id,
        replace_existing=True,
        name=f"{scrapper.name} [{cron}]",
    )
