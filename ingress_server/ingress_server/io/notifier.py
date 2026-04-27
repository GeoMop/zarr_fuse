import smtplib
import logging

from email.message import EmailMessage

from ..app_config import SmtpConfig

LOG = logging.getLogger(__name__)


def send_failure_email(
    *,
    smtp_config: SmtpConfig,
    scrapper_name: str,
    cron: str,
    failures: list[dict],
) -> None:
    if not smtp_config.enabled:
        LOG.info(
            "Active scrapper notifications disabled for %s: smtp not configured",
            scrapper_name,
        )
        return

    msg = EmailMessage()
    msg["Subject"] = f"[ingress] Active scrapper failed: {scrapper_name}"
    msg["From"] = smtp_config.from_email or smtp_config.username
    msg["To"] = ", ".join(smtp_config.notify_to)

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

    with smtplib.SMTP(smtp_config.host, smtp_config.port, timeout=30) as smtp:
        smtp.starttls()
        smtp.login(smtp_config.username, smtp_config.password)
        smtp.send_message(msg)

    LOG.info(
        "Failure notification email sent for scrapper %s to %s",
        scrapper_name,
        smtp_config.notify_to,
    )
