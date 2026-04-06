import os
import smtplib
import logging

from dotenv import load_dotenv
from email.message import EmailMessage

load_dotenv()

LOG = logging.getLogger(__name__)


def _get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required env var: {name}")
    return value


def send_failure_email(
    *,
    scrapper_name: str,
    cron: str,
    failures: list[dict],
) -> None:
    recipients_raw = os.getenv("ACTIVE_SCRAPPER_NOTIFY_TO", "").strip()
    if not recipients_raw:
        LOG.info(
            "Active scrapper notifications disabled for %s: ACTIVE_SCRAPPER_NOTIFY_TO is empty",
            scrapper_name,
        )
        return

    recipients = [x.strip() for x in recipients_raw.split(",") if x.strip()]
    if not recipients:
        LOG.info(
            "Active scrapper notifications disabled for %s: no valid recipients",
            scrapper_name,
        )
        return

    smtp_host = _get_required_env("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = _get_required_env("SMTP_USER")
    smtp_password = _get_required_env("SMTP_PASSWORD")
    mail_from = os.getenv("SMTP_FROM", smtp_user).strip()

    msg = EmailMessage()
    msg["Subject"] = f"[ingress] Active scrapper failed: {scrapper_name}"
    msg["From"] = mail_from
    msg["To"] = ", ".join(recipients)

    lines = [
        f"Active scrapper '{scrapper_name}' had {len(failures)} failed context(s).",
        f"Cron: {cron}",
        "",
        "Failures:",
        "",
    ]

    for idx, failure in enumerate(failures, start=1):
        lines.extend([
            f"{idx}. type: {failure['type']}",
            f"   error: {failure['error']}",
            f"   context: {failure['context']}",
            "",
        ])

    msg.set_content("\n".join(lines))

    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as smtp:
        smtp.starttls()
        smtp.login(smtp_user, smtp_password)
        smtp.send_message(msg)

    LOG.info(
        "Failure notification email sent for scrapper %s to %s",
        scrapper_name,
        recipients,
    )
