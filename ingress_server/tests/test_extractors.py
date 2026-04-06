from pathlib import Path

from ingress_server.models import MetadataModel
from ingress_server.io.extractor import apply_extractor

import inspect


def test_extractor_from_inputs_dir():
    test_dir = Path(__file__).parent
    inputs_dir = test_dir / "inputs"

    meta_path = test_dir / "data/20251124T011503_3b200e72354d.json.meta.json"
    metadata = MetadataModel.model_validate_json(meta_path.read_text(encoding="utf-8"))

    payload = b'{}'

    print("apply_extractor object:", apply_extractor)
    print("apply_extractor module:", apply_extractor.__module__)
    print("apply_extractor signature:", inspect.signature(apply_extractor))

    result = apply_extractor(
        payload=payload,
        metadata=metadata,
        fallback_config_dir=inputs_dir,
    )

    assert result is not None
