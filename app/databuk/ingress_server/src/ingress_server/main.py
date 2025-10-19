import atexit
import logging
import os

import uvicorn
from fastapi import FastAPI
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler

from ingress_server.web import api
from ingress_server.scrapper import scheduler
from common import configuration, logging as config_logging

APP = FastAPI(title="Databuk Ingress Server", version="2.0.0")
BG_SCHEDULER = BackgroundScheduler()
LOG = logging.getLogger("ingress-main")

load_dotenv()


@APP.get("/health")
def health():
    return {"status": "ok"}

def create_app():
    LOG.info("Creating FastAPI application")

    config_logging.setup_logging(os.getenv("LOG_LEVEL", "INFO"))

    endpoints = configuration.load_endpoints_config()
    for ep in endpoints:
        LOG.info("Registering endpoint %s at %s", ep.name, ep.endpoint)
        api.register_upload_endpoints(APP, ep.name, ep.endpoint, ep.schema_path)

    scrappers = configuration.load_scrappers_config()
    scheduler.start_scrapper_jobs(BG_SCHEDULER, scrappers)

    BG_SCHEDULER.start()
    LOG.info("Scheduled %d scrappers", len(scrappers))

def main():
    create_app()
    uvicorn.run(APP, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))

if __name__ == "__main__":
    main()
