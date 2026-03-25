import logging
from pathlib import Path

from ingress_server.app_config import AppConfig, load_app_config
from ingress_server.worker import _process_one

LOG = logging.getLogger(__name__)

def prepare_test_environment() -> AppConfig:
    script_dir = Path(__file__).parent
    config_path = script_dir / "inputs" / "endpoints_config.yaml"
    queue_dir = script_dir / "workdir" / "queue"

    return load_app_config(config_path, queue_dir)

def test_worker_process_one_with_json_payload():
    app_config = prepare_test_environment()

    data_path = app_config.config_dir / "data/20251124T011503_3b200e72354d.json"
    LOG.info(f"Testing _process_one with data path: {data_path}")

    _process_one(app_config, data_path)
