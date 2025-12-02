import logging
from logging import Logger, DEBUG, Formatter, Handler, StreamHandler
import threading
import asyncio
import sys
from asyncio import wait_for

import zarr
CpuBuffer = zarr.core.buffer.cpu.Buffer
from datetime import datetime, timezone


def get_logger(store, path: str, name: str | None = None) -> Logger:
    """
    Create and return a logger.

    If `store` is not None, logs are written into the given Zarr store via
    StoreLogHandler. If `store` is None, logs go to stderr via StderrLogHandler,
    but with the same formatter (incl. [path] context).

    Parameters
    ----------
    store : zarr.abc.Store or None
        A Zarr 3 store (DirectoryStore, FSStore, MemoryStore, etc.) or None.
    path : str
        Group/path context for the logger; appears in log messages.
    name : str, optional
        Name for the logger. If None, a name based on the store id and path is used.

    Returns
    -------
    logging.Logger
        Configured logger instance.
    """
    suffix = "stderr" if store is None else str(id(store))
    logger_name = name or f"zarr_logger_{suffix}{path}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(DEBUG)
    logger.propagate = False

    # Remove any existing handlers (to avoid duplicate logs)
    for h in list(logger.handlers):
        logger.removeHandler(h)

    if store is None:
        handler: Handler = StderrLogHandler(path)
    else:
        handler = StoreLogHandler(store, path)

    logger.addHandler(handler)
    return logger


class _BaseFormatterMixin:
    """Mixin to provide the common formatter used by both handlers."""

    def _setup_formatter(self, group_path: str):
        fmt = f"%(asctime)s %(levelname)-5s [{group_path}] %(message)s"
        formatter = Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S")
        self.setFormatter(formatter)


class StderrLogHandler(StreamHandler, _BaseFormatterMixin):
    """
    Fallback handler used when store is None.

    Writes to stderr but uses the same format as StoreLogHandler.
    """

    def __init__(self, group_path: str):
        super().__init__(stream=sys.stderr)
        self.group_path = group_path
        self.prefix = "logs"  # unused, kept for symmetry
        self._setup_formatter(group_path)


class StoreLogHandler(Handler, _BaseFormatterMixin):
    """
    Zarr-3 handler that writes into logs/YYYYMMDD.log via a private
    background asyncio loop. DEBUG/ERROR block until written (with
    full traceback printed on error); INFO/WARNING fire-and-forget.

    Starts with a partial-write (_append_unsafe), but on any failure
    that method will swap itself out for the full-read/write (_append_safe).
    """

    def __init__(self, store, group_path: str, partial_write: bool = False):
        super().__init__()
        self.store = store
        self.group_path = group_path
        self.prefix = "logs"

        self._setup_formatter(group_path)

        # start private asyncio loop in a thread
        self._append_fut = None
        self._loop = asyncio.new_event_loop()
        threading.Thread(target=self._loop.run_forever, daemon=True).start()
        self._schedule = lambda coro: asyncio.run_coroutine_threadsafe(coro, self._loop)

        # choose initial append strategy

        if isinstance(store, (zarr.storage.LocalStore,)) and getattr(store, 'supports_partial_writes', False):
            # still work on LocalStore.
            self._append = self._append_unsafe
        else:
            self._append = self._append_safe

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
            print(f"[DEBUG] Calling store.get - key: {key}, buffer_prototype: {self._buffer_prototype}, store_type: {type(self.store)}")
            buf_old = await self.store.get(key, self._buffer_prototype)
            bytes_old = buf_old.as_numpy_array().tobytes()
            bytes_new = buf.as_numpy_array().tobytes()
            concat = bytes_old + bytes_new
            buf = CpuBuffer.from_bytes(concat)

        await self.store.set(key, buf)

    def wait_for_last_message(self):
        if self._append_fut is None:
            return
        try:
            self._append_fut.result()
        except Exception:
            # print full traceback for diagnostics
            sys.excepthook(*sys.exc_info())

    def _schedule_and_maybe_wait(self, key: str, buf: CpuBuffer, wait: bool):
        # wait for previous message to complete
        self.wait_for_last_message()
        self._append_fut = self._schedule(self._append(key, buf))
        if wait:
            self.wait_for_last_message()

    def emit(self, record):
        raw = (self.format(record) + "\n").encode("utf-8")
        buf = CpuBuffer.from_bytes(raw)
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc)
        day = ts.strftime("%Y%m%d")
        key = f"{self.prefix}/{day}.log"
        wait = record.levelno in (logging.DEBUG, logging.ERROR)
        self._schedule_and_maybe_wait(key, buf, wait)

    def close(self):
        super().close()
        self._loop.call_soon_threadsafe(self._loop.stop)
