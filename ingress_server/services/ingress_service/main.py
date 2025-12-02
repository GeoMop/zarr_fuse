import logging
import os
import uvicorn

from fastapi import FastAPI
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler

from .web import api
from .scrapper import scheduler

from packages.common import configuration, logging_setup

APP = FastAPI(title="Databuk Ingress Server", version="2.0.0")
BG_SCHEDULER = BackgroundScheduler()
LOG = logging.getLogger("ingress-main")

load_dotenv()


@APP.get("/health")
def health():
    return {"status": "ok"}

def create_app():
    LOG.info("Creating FastAPI application")

    logging_setup.setup_logging(os.getenv("LOG_LEVEL", "INFO"))

    endpoints = configuration.load_endpoints_config()
    for ep in endpoints:
        LOG.info("Registering endpoint %s at %s", ep.name, ep.endpoint)
        api.register_upload_endpoints(APP, ep.name, ep.endpoint, ep.schema_name)

    scrappers = configuration.load_scrappers_config()
    for scrapper in scrappers:
        LOG.info("Configured scrapper job %s with cron '%s'", scrapper.name, scrapper.cron)
        scheduler.add_scrapper_job(BG_SCHEDULER, scrapper)

    BG_SCHEDULER.start()
    LOG.info("Scheduled %d scrappers", len(scrappers))

def main():
    create_app()
    uvicorn.run(APP, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))

if __name__ == "__main__":
    main()
