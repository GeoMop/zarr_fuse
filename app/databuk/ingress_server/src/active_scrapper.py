import requests
import logging
import json
import time
import logging

from io_utils import (
    validate_content_type,
    validate_data,
    sanitize_node_path,
    atomic_write,
    new_msg_path,
)
from pathlib import Path
from configs import ACCEPTED_DIR
from configs import CONFIG
from apscheduler.schedulers.background import BackgroundScheduler

LOG = logging.getLogger("active_scrapper")
scheduler = BackgroundScheduler()


def _save_payload(
    name: str,
    schema_path: str,
    data: bytes,
    content_type: str = "application/json",
    node_path: str = "",
    username: str = "scrapper",
) -> tuple[Path, str | None]:
    content_type = (content_type or "").lower()

    ok, err = validate_content_type(content_type)
    if not ok:
        return None, f"Invalid content type: {err}"

    ok, err = validate_data(data, content_type)
    if not ok:
        return None, f"Invalid data: {err}"

    safe_child, err = sanitize_node_path(node_path)
    if err:
        return None, f"Invalid node_path: {err}"

    base = (ACCEPTED_DIR / name) / safe_child
    suffix = ".csv" if "csv" in content_type else ".json"
    msg_path = new_msg_path(base, suffix)

    atomic_write(msg_path, data)

    meta_data = {
        "content_type": content_type,
        "node_path": node_path,
        "name": name,
        "username": username,
        "received_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "schema_path": schema_path,
    }
    atomic_write(
        msg_path.with_suffix(msg_path.suffix + ".meta.json"),
        json.dumps(meta_data).encode("utf-8"),
    )

    LOG.info(
        "Accepted(scrapper) endpoint=%s node_path=%s path=%s ct=%s bytes=%d",
        name,
        node_path,
        msg_path,
        content_type,
        len(data),
    )
    return msg_path, None

def _run_scrapper(job):
    r = requests.get(job["url"], timeout=30)
    r.raise_for_status()

    payload = r.content

    _save_payload(
        name=job["name"],
        schema_path=job["schema_path"],
        data=payload,
        content_type=r.headers.get("Content-Type", "application/json"),
        node_path=job.get("node_path", ""),
        username="scrapper",
    )

def start_scrapper_jobs():
    config = CONFIG.get("active_scrappers", [])
    if not config:
        LOG.info("No active scrapper jobs configured")
        return

    for job in config:
        cron_expr: str = job["cron"]
        if not cron_expr:
            LOG.warning("Scrapper %s has no cron expression - skipping", job["name"])
            continue

        minute, hour, day, month, dow = cron_expr.split()
        scheduler.add_job(
            _run_scrapper,
            "cron",
            args=[job],
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=dow,
            id=f"scrapper-{job['name']}",
            replace_existing=True,
        )
    scheduler.start()
    LOG.info("Scheduled %d scrappers", len(config))
