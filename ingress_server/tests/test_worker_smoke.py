import logging
from pathlib import Path

from ingress_server.app_config import AppConfig, load_app_config
from ingress_server.worker import _process_one
from ingress_server.worker import _process_available_files

LOG = logging.getLogger(__name__)


def prepare_test_environment() -> AppConfig:
    test_dir = Path(__file__).parent
    config_path = test_dir / "inputs" / "endpoints_config.yaml"
    queue_dir = test_dir / "workdir" / "queue"
    return load_app_config(config_path, queue_dir)


def test_worker_process_one_with_json_payload() -> None:
    test_dir = Path(__file__).parent
    app_config = prepare_test_environment()

    data_path = test_dir / "data" / "20251124T011503_3b200e72354d.json"
    LOG.info("Testing _process_one with data path: %s", data_path)

    _process_one(app_config, data_path)


def test_extractor_from_inputs_dir():
    app_config = prepare_test_environment()

    _process_available_files(app_config)
