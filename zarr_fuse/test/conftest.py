import os
import pytest
from pathlib import Path

@pytest.fixture
def smart_tmp_path(request):
    use_real_tmp = os.environ.get("DEBUG_TEST_TMP") == "0"

    if use_real_tmp:
        # Use pytest's own temp dir in CI or strict mode
        yield request.getfixturevalue("tmp_path")
    else:
        # Use persistent workdir for local/dev runs
        script_dir = Path(__file__).parent
        workdir = script_dir / "workdir"
        workdir.mkdir(parents=True, exist_ok=True)
        yield workdir