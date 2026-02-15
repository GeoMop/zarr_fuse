import logging
from pathlib import Path

from ingress_server.configs import Settings, init_settings
from ingress_server.worker import _process_one

LOG = logging.getLogger(__name__)

def prepare_test_environment() -> Settings:
    script_dir = Path(__file__).parent
    inputs_dir = script_dir / "inputs"
    queue_dir = script_dir / "workdir" / "queue"

    settings = init_settings(queue_dir=queue_dir, config_dir=inputs_dir)
    return settings

def test_worker_process_one_with_json_payload():
    settings = prepare_test_environment()

    data_path = settings.config_dir / "data/20251124T011503_3b200e72354d.json"
    LOG.info(f"Testing _process_one with data path: {data_path}")

    err = _process_one(data_path)
    assert err is None
