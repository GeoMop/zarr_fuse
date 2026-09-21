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

from zarr_fuse.logger import StoreLogHandler, get_logger
from zarr_fuse.zarr_storage import _zarr_store_open
import zarr_fuse as zf

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


def test_make_child_logger(tmp_path):
    """
    Open schema_tree.yaml with a local store, verify:
    1. _make_consistent builds the expected child tree.
    2. All child nodes share the root's logger instance (no extra loggers/threads).
    3. node.logger.info() writes to the store (StoreLogHandler works end-to-end).
    4. Calling get_logger again (simulating a re-open) does not accumulate handlers
       — old ones are properly closed before a new handler is attached.
    """
    schema = zf.schema.deserialize(inputs_dir / "schema_tree.yaml")
    store_path = tmp_path / "make_child_test.zarr"
    node = zf.open_store(schema, STORE_URL=str(store_path))

    # --- 1. child tree structure from _make_consistent ---
    assert "child_1" in node.children
    assert "child_2" in node.children
    assert "child_3" in node.children["child_1"].children

    # --- 2. children share the root logger (make_child passes self.logger, not self._logger) ---
    root_logger = node.logger
    assert node.children["child_1"].logger is root_logger
    assert node.children["child_2"].logger is root_logger
    assert node.children["child_1"].children["child_3"].logger is root_logger

    # --- 3. logger.info() reaches the store ---
    root_logger.info("hello from root")

    handler = next(h for h in root_logger.handlers if isinstance(h, StoreLogHandler))
    handler.wait_for_last_message()

    zarr_root_store = zarr.open_group(node.store).store
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    buf = asyncio.run(zarr_root_store.get(f"logs/{today}.log", default_buffer_prototype()))
    text = buf.as_array_like().tobytes().decode("utf-8")
    assert "hello from root" in text

    # --- 4. get_logger does not accumulate handlers when called again ---
    same_logger = get_logger(zarr_root_store, node.group_path)
    assert len(same_logger.handlers) == 1, (
        f"Expected exactly 1 handler after re-calling get_logger, got {len(same_logger.handlers)}"
    )