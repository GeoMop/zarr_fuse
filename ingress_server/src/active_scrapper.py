import time
import json
import logging
import requests

import logging

from io_utils import validate_content_type, atomic_write, new_msg_path, validate_data
from configs import ACCEPTED_DIR

from apscheduler.schedulers.background import BackgroundScheduler

BG_SCHEDULER = BackgroundScheduler()
LOG = logging.getLogger("active-scrapper")

def _call_method(url: str, method: str) -> requests.Response:
    if method.upper() == "GET":
        return requests.get(url, timeout=30)
    elif method.upper() == "POST":
        return requests.post(url, timeout=30)
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")

def run_job(name: str, url: str, method: str, schema_path: str, extract_fn: str = None, fn_module: str = None):
    try:
        response = _call_method(url, method)
        response.raise_for_status()
    except Exception as e:
        LOG.warning("Scrapper job %s failed to fetch %s: %s", name, url, e)
        return

    content_type = response.headers.get("Content-Type", "application/json")

    ok, err = validate_content_type(content_type)
    if not ok:
        LOG.warning("Validation content type failed for %s: %s", content_type, err)
        return

    payload = response.content
    ok, err = validate_data(payload, content_type)
    if not ok:
        LOG.warning("Validating data failed for %s", err)
        return

    base = (ACCEPTED_DIR / name)
    suffix = ".csv" if "csv" in content_type else ".json"
    msg_path = new_msg_path(base, suffix)

    atomic_write(msg_path, payload)

    meta_data = {
        "extract_fn": extract_fn,
        "fn_module": fn_module,
        "content_type": content_type,
        "node_path": "",
        "endpoint_name": name,
        "username": f"scrapper-{name}",
        "received_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "schema_path": schema_path,
    }

    atomic_write(msg_path.with_suffix(msg_path.suffix + ".meta.json"), json.dumps(meta_data).encode("utf-8"))

    LOG.info("Scrapper accepted name=%s loc=%s", name, msg_path)

def add_scrapper_job(name: str, url: str, cron: str, schema_path: str, method: str = "GET", extract_fn: str = None, fn_module: str = None):
    minute, hour, day, month, dow = cron.split()
    BG_SCHEDULER.add_job(
        run_job,
        "cron",
        args=[
            name,
            url,
            method,
            schema_path,
            extract_fn,
            fn_module,
        ],
        minute=minute, hour=hour, day=day, month=month, day_of_week=dow,
        id=f"scrapper-{name}", replace_existing=True,
        name=name,
    )
