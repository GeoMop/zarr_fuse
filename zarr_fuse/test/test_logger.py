import time
from typing import MutableMapping
import logging
import re
from pathlib import Path
import pytest

from datetime import datetime, timezone
import zarr
from zarr.core.buffer.core import default_buffer_prototype

import asyncio

from zarr_fuse.logger import StoreLogHandler  # adjust to your import path
from zarr_fuse.zarr_storage import _zarr_store_open

script_dir = Path(__file__).parent
inputs_dir = script_dir / "inputs"
workdir = script_dir / "workdir"

@pytest.mark.parametrize("store_options", [
    {'STORE_URL':str(workdir/"log_store.zarr")},     # on‐disk DirectoryStore
])
def test_store_log_handler(tmp_path, store_options):


    # ─── build the store based on our “url” ─────────────────────────────
    store = _zarr_store_open(store_options)
    # cleanup
    zarr.open_group(store, mode='w')

    # create our handler
    node_path="/yr_no"
    handler = StoreLogHandler(store, node_path)

    # minimal sanity on the handler
    assert handler.store is store
    assert handler.prefix == "logs"
    assert isinstance(handler.formatter, logging.Formatter)
    assert "%(message)s" in handler.formatter._fmt

    # wire up a fresh logger
    logger = logging.getLogger(f"test_{id(store)}")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    # remove any existing handlers
    for h in list(logger.handlers):
        logger.removeHandler(h)
    logger.addHandler(handler)

    # emit three records
    logger.info("First message")
    logger.debug("Fourth message")
    logger.warning("Second message")
    logger.error("Third message")
    logger.info("Fifth message")
    time.sleep(1)

    # ─── identify the log key for today in UTC ────────────────────────
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    key = f"{handler.prefix}/{today}.log"


    # Async‐store access with BufferPrototype
    buf = asyncio.run(store.get(key, default_buffer_prototype()))
    raw = buf.as_array_like().tobytes()
# ─── verify log contents ──────────────────────────────────────────
    text = raw.decode("utf-8")
    lines = text.strip().split("\n")
    # should contain all five messages in order
    assert len(lines) == 5

    expected = [
        ("INFO ", "First message"),
        ("DEBUG", "Fourth message"),
        ("WARNING", "Second message"),
        ("ERROR", "Third message"),
        ("INFO ", "Fifth message"),
    ]
    timestamp_re = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
    for line, (lvl, msg) in zip(lines, expected):
        # starts with a timestamp
        assert timestamp_re.match(line)
        # ends with "LEVEL message"
        assert line.endswith(f"{lvl} [{node_path}] {msg}")