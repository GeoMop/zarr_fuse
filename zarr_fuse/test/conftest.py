import os
import pytest
from pathlib import Path

@pytest.fixture
def smart_tmp_path(request):
    # Use persistent workdir for local/dev runs
    script_dir = Path(__file__).parent
    workdir = script_dir / "workdir"
    workdir.mkdir(parents=True, exist_ok=True)
    yield workdir