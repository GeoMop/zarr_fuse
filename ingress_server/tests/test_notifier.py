"""
Manual test — sends a real failure email via SMTP.
Requires SMTP_PASSWORD in .env and valid smtp config in inputs/endpoints_config.yaml.

Run explicitly:
    pytest tests/test_notifier.py -v -s
"""
import pytest
from pathlib import Path

from ingress_server.app_config import load_app_config
from ingress_server.io.notifier import send_anomaly_email, send_failure_email


@pytest.fixture
def smtp_config():
    config_path = Path(__file__).parent.parent / "inputs" / "endpoints_config.yaml"
    if not config_path.exists():
        pytest.skip(f"No local ingress configuration at {config_path}")
    app_config = load_app_config(config_path)
    return app_config.smtp


def test_send_failure_email(smtp_config):
    if not smtp_config.host or not smtp_config.notify_to:
        pytest.skip("SMTP not configured (host or notify_to missing)")
    if not smtp_config.password:
        pytest.skip("SMTP_PASSWORD not set in .env")

    send_failure_email(
        smtp_config=smtp_config,
        scrapper_name="test-scrapper",
        cron="0 12 * * *",
        failures=[
            {
                "type": "unexpected",
                "error": "Connection timeout after 30s",
                "context": {"lat": "49.2", "lon": "16.6"},
            },
            {
                "type": "payload_validation",
                "error": "Missing required field 'temperature'",
                "context": {"lat": "50.1", "lon": "14.4"},
            },
        ],
    )


def test_send_anomaly_email(smtp_config):
    if not smtp_config.host or not smtp_config.notify_to:
        pytest.skip("SMTP not configured (host or notify_to missing)")
    if not smtp_config.password:
        pytest.skip("SMTP_PASSWORD not set in .env")

    send_anomaly_email(
        smtp_config=smtp_config,
        anomalies=[
            {
                "type": "time_key",
                "error": "time_like_coord='date_time': unparseable time string 'yesterday'",
                "context": {"file": "20250919T111523_177cbe0317bd.json", "endpoint": "bukov"},
            },
            {
                "type": "time_key_type_conflict",
                "error": "batch mixes time key types: datetime (a.json), numeric (b.json)",
                "context": {"batch_size": 2},
            },
        ],
    )
