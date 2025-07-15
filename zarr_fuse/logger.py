# setup of global logging
#
# Usage in a module:
# from chodby_inv import get_logger
# logger = get_logger(__name__)
#
# def foo():
#     logger.info("Info message")
#     logger.debug("Debug message")
#
# This makes a logger that automaticaly adds the module context to the
# messages, while the log file as well as formating remains common according to the
# configuration in this file.
#
# TODO:
#  - Logger still does not support more then one process, improve that.
#    probably using an array would be best way to go. Splitting long messages if necessary.
#    Appends should be "signed" somehow so we can hash author processes
#  - Message columns: data_time, level, process_hash =
#    (author_hash, computer_hash, process_hash), message_id,
#


import logging
import threading
import asyncio
import sys
import traceback
from asyncio import wait_for

import zarr
CpuBuffer = zarr.core.buffer.cpu.Buffer
from datetime import datetime, timezone


def get_logger(store, path, name: str = None) -> logging.Logger:
    """
    Create and return a logger that writes into the given Zarr store
    via StoreLogHandler.

    Parameters
    ----------
    store : zarr.abc.Store
        A Zarr 3 store (DirectoryStore, FSStore, MemoryStore, etc.)
    name : str, optional
        Name for the logger. If None, a name based on the store id is used.

    Returns
    -------
    logging.Logger
        Configured logger instance.
    """
    logger_name = name or f"zarr_logger_{id(store)}{path}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    # Remove any existing handlers (to avoid duplicate logs)
    for h in list(logger.handlers):
        logger.removeHandler(h)

    # Attach our StoreLogHandler
    handler = StoreLogHandler(store, path)
    logger.addHandler(handler)

    return logger


class StoreLogHandler(logging.Handler):
    """
    Zarr-3 handler that writes into logs/YYYYMMDD.log via a private
    background asyncio loop. DEBUG/ERROR block until written (with
    full traceback printed on error); INFO/WARNING fire-and-forget.

    Starts with a partial-write (_append_unsafe), but on any failure
    that method will swap itself out for the full-read/write (_append_safe).
    """

    def __init__(self, store, group_path):
        super().__init__()
        self.store = store
        self.group_path = group_path
        self.prefix = "logs"
        self.setFormatter(logging.Formatter(
            f"%(asctime)s %(levelname)-5s [{self.group_path}] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))

        try:
            asyncio.get_running_loop()
            self._mode = "async"
        except RuntimeError:
            self._mode = "sync"

        self._mode = "sync"
        if self._mode == "async":
            # Initially, a completed future so the first message doesn't wait.
            self._last_future = asyncio.get_event_loop().create_future()
            self._last_future.set_result(None)
            self._emit_fn = self._emit_async
        else:
            self._emit_fn = self._emit_sync


        self._append = self._append_safe
        self._buffer_prototype = zarr.core.buffer.core.default_buffer_prototype()

    def emit(self, record):
        raw = (self.format(record) + "\n").encode("utf-8")
        buf = CpuBuffer.from_bytes(raw)
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc)
        day = ts.strftime("%Y%m%d")
        key = f"{self.prefix}/{day}.log"

        wait = record.levelno in (logging.DEBUG, logging.ERROR)
        if wait:
            self._emit_sync(key, buf)
        else:
            self._emit_fn(key, buf)

    def _emit_sync(self, key, buf):
        # Always blocking; runs the append coroutine and waits for completion
        asyncio.run(self._append(key, buf))

    def _emit_async(self, key, buf):
        loop = asyncio.get_running_loop()
        fut = loop.create_task(self.chained_append(key, buf))
        self._last_future = fut

    def _report_logging_error(self, message):
        print(f"[StoreLogHandler] {message}", file=sys.stderr)
        import traceback
        traceback.print_exc()

    async def chained_append(self, key, buf):
        try:
            await self._last_future
        except Exception:
            self._report_logging_error("Previous log write failed")
        try:
            await self._append(key, buf)
        except Exception:
            self._report_logging_error("Logging failed; see traceback below")


    async def _append_unsafe(self, key: str, buf: CpuBuffer):
        """
        Try a partial-write; on any exception, switch to safe mode
        and replay this same append in safe mode.
        """
        try:
            exists = await self.store.exists(key)
            if not exists:
                # first write must be full rewrite
                await self._append_safe(key, buf)
                return
            offset = await self.store.getsize(key) if exists else 0
            await self.store.set_partial_values([(key, offset, buf)])
        except Exception:
            # print full traceback for diagnostics
            sys.excepthook(*sys.exc_info())
            # swap in safe method permanently
            self._append = self._append_safe
            # replay once in safe mode
            await self._append_safe(key, buf)

    async def _append_safe(self, key: str, buf: CpuBuffer):
        """Full read+concatenate+write fallback."""
        exists = await self.store.exists(key)

        if exists:
            buf_old = await self.store.get(key, self._buffer_prototype)
            bytes_old = buf_old.as_numpy_array().tobytes()
            bytes_new = buf.as_numpy_array().tobytes()
            concat = bytes_old + bytes_new
            print("\n", str(concat))
            buf = CpuBuffer.from_bytes(concat)

        await self.store.set(key, buf)


    def close(self):
        super().close()
        last_fut = getattr(self, "_last_future", None)
        if last_fut is not None and not last_fut.done():
            try:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(last_fut)
            except Exception:
                self._report_logging_error("Error while closing; see traceback below")