"""
Local copies of the queue payloads (issue #113).

The queue storage (S3 in production) stays the source of truth. The receipt
path puts a copy of every payload here, and the worker reads it instead of
downloading the payload, but only when the SHA-256 digest of the copy matches
the one recorded in the manifest. A missing, stale or partially written copy
therefore just falls back to the queue storage. Every operation is best
effort: a cache failure is logged, never raised.

NOTE: this module must not import anything from the ingress_server package
(app_config imports it, while io.* modules import app_config).
"""
import os
import uuid
import hashlib
import logging
import contextlib

from pathlib import Path

LOG = logging.getLogger(__name__)

TMP_SUFFIX = ".tmp"


def content_hash(payload: bytes) -> str:
    """SHA-256 hex digest identifying a payload in the manifest and the cache."""
    return hashlib.sha256(payload).hexdigest()


class LocalCache:
    """Payload copies keyed by the queue item name; disabled without a directory."""

    def __init__(self, cache_dir: str | Path | None):
        self.root = Path(cache_dir).resolve() if cache_dir else None

        if self.root is not None:
            try:
                self.root.mkdir(parents=True, exist_ok=True)
            except OSError:
                LOG.exception("Unusable cache directory %s, the local cache is disabled", self.root)
                self.root = None

    @property
    def enabled(self) -> bool:
        return self.root is not None

    def _path(self, name: str) -> Path | None:
        """Path of a cached item; None for a name escaping the cache directory."""
        path = (self.root / name).resolve()
        if not path.is_relative_to(self.root):
            LOG.error("Refusing to cache item %r outside of %s", name, self.root)
            return None
        return path

    def put(self, name: str, payload: bytes) -> None:
        """
        Store a copy. Written to a temporary file of its own first, so that a
        reader never sees a torn copy and concurrent puts of the same item
        (the receipt path and the worker) do not collide.
        """
        if not self.enabled:
            return

        path = self._path(name)
        if path is None:
            return

        tmp_path = path.with_name(f"{path.name}.{uuid.uuid4().hex}{TMP_SUFFIX}")
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path.write_bytes(payload)
            os.replace(tmp_path, path)
        except OSError:
            LOG.exception("Failed to cache %s", name)
            with contextlib.suppress(OSError):
                tmp_path.unlink(missing_ok=True)

    def get(self, name: str, sha256: str) -> bytes | None:
        """The cached copy if its digest is `sha256`, None otherwise (or if `sha256` is unknown)."""
        if not self.enabled or not sha256:
            return None

        path = self._path(name)
        if path is None:
            return None

        try:
            payload = path.read_bytes()
        except FileNotFoundError:
            return None
        except OSError:
            LOG.exception("Failed to read the cached copy of %s", name)
            return None

        if content_hash(payload) != sha256:
            LOG.warning("Cached copy of %s does not match its content hash, ignored", name)
            return None
        return payload

    def evict(self, name: str) -> None:
        if not self.enabled:
            return

        path = self._path(name)
        if path is None:
            return

        try:
            path.unlink(missing_ok=True)
        except OSError:
            LOG.exception("Failed to evict the cached copy of %s", name)

    def sweep(self, keep: set[str]) -> None:
        """
        Delete the copies of items not in `keep` and leftover temporary files,
        e.g. copies of items that left the queue while the worker was down.
        """
        if not self.enabled:
            return

        try:
            paths = [path for path in self.root.rglob("*") if path.is_file()]
        except OSError:
            LOG.exception("Failed to list the cache directory %s", self.root)
            return

        for path in paths:
            name = path.relative_to(self.root).as_posix()
            if name in keep and not name.endswith(TMP_SUFFIX):
                continue
            try:
                path.unlink(missing_ok=True)
            except OSError:
                LOG.exception("Failed to sweep the cached file %s", path)
