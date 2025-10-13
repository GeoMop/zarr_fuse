import atexit
import logging

from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler

from ingress_server.web import api
from ingress_server.scrapper import scheduler
from common import configuration, logging as config_logging

APP = Flask(__name__)
BG_SCHEDULER = BackgroundScheduler()
LOG = logging.getLogger("ingress-main")

@APP.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

def create_app() -> Flask:
    LOG.info("Creating Flask application")

    cfg = configuration.load_app_config()
    config_logging.setup_logging(cfg.log_level)
    APP.config["APP_CONFIG"] = cfg

    endpoints = configuration.load_endpoints_config()
    for ep in endpoints:
        LOG.info("Registering endpoint %s at %s", ep.name, ep.endpoint)
        api.register_upload_endpoints(APP, ep.name, ep.endpoint, ep.schema_path)

    scrappers = configuration.load_scrappers_config()
    scheduler.start_scrapper_jobs(BG_SCHEDULER, scrappers)

    BG_SCHEDULER.start()
    LOG.info("Scheduled %d scrappers", len(scrappers))

    return APP

def _graceful_shutdown():
    try:
        BG_SCHEDULER.shutdown(wait=False)
    except Exception as e:
        LOG.error("Error shutting down scheduler: %s", e)

def main():
    app = create_app()

    atexit.register(_graceful_shutdown)

    port = app.config["APP_CONFIG"].port
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

if __name__ == "__main__":
    main()
