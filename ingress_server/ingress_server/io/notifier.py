import smtplib
import logging

from email.message import EmailMessage

from ..app_config import SmtpConfig

LOG = logging.getLogger(__name__)


def _smtp_configured(smtp_config: SmtpConfig) -> bool:
    return bool(smtp_config.password and smtp_config.notify_to and smtp_config.host)


def _send(smtp_config: SmtpConfig, subject: str, body: str) -> None:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_config.from_email or smtp_config.username
    msg["To"] = ", ".join(smtp_config.notify_to)
    msg.set_content(body)

    with smtplib.SMTP(smtp_config.host, smtp_config.port, timeout=30) as smtp:
        smtp.starttls()
        smtp.login(smtp_config.username, smtp_config.password)
        smtp.send_message(msg)


def _entry_lines(entries: list[dict]) -> list[str]:
    lines: list[str] = []
    for idx, entry in enumerate(entries, start=1):
        lines.extend([
            f"{idx}. type: {entry['type']}",
            f"   error: {entry['error']}",
            f"   context: {entry['context']}",
            "",
        ])
    return lines


def send_failure_email(
    *,
    smtp_config: SmtpConfig,
    scrapper_name: str,
    cron: str,
    failures: list[dict],
) -> None:
    if not _smtp_configured(smtp_config):
        LOG.info(
            "Active scrapper notifications disabled for %s: smtp not configured",
            scrapper_name,
        )
        return

    lines = [
        f"Active scrapper '{scrapper_name}' had {len(failures)} failed context(s).",
        f"Cron: {cron}",
        "",
        "Failures:",
        "",
    ]
    lines.extend(_entry_lines(failures))

    _send(
        smtp_config,
        f"[ingress] Active scrapper failed: {scrapper_name}",
        "\n".join(lines),
    )

    LOG.info(
        "Failure notification email sent for scrapper %s to %s",
        scrapper_name,
        smtp_config.notify_to,
    )


def send_anomaly_email(*, smtp_config: SmtpConfig, anomalies: list[dict]) -> None:
    """Report atypical data the worker kept processing anyway — an unreadable
    time coordinate, sources disagreeing on the time key type, and the like."""
    if not _smtp_configured(smtp_config):
        LOG.info("Data anomaly notifications disabled: smtp not configured")
        return

    lines = [
        f"The ingress worker detected {len(anomalies)} data anomaly(ies) "
        f"while filtering a batch by data time.",
        "",
        "Anomalies:",
        "",
    ]
    lines.extend(_entry_lines(anomalies))

    _send(
        smtp_config,
        f"[ingress] Data anomalies detected ({len(anomalies)})",
        "\n".join(lines),
    )

    LOG.info("Data anomaly notification email sent to %s", smtp_config.notify_to)
