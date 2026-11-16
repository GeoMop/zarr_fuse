"""Local copies of the queue payloads, used only when their SHA-256 matches the manifest."""
import os
import uuid
import hashlib
import logging
import contextlib

from pathlib import Path

LOG = logging.getLogger(__name__)


def content_hash(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


class LocalCache:
    def __init__(self, cache_dir: str | Path | None):
        self.root = Path(cache_dir) if cache_dir else None

    def put(self, name: str, payload: bytes) -> None:
        if self.root is None:
            return

        path = self.root / name
        tmp_path = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path.write_bytes(payload)
            os.replace(tmp_path, path)
        except OSError:
            LOG.exception("Failed to cache %s", name)
            with contextlib.suppress(OSError):
                tmp_path.unlink(missing_ok=True)

    def get(self, name: str, sha256: str) -> bytes | None:
        if self.root is None or not sha256:
            return None

        try:
            payload = (self.root / name).read_bytes()
        except OSError:
            return None
        return payload if content_hash(payload) == sha256 else None

    def evict(self, name: str) -> None:
        if self.root is None:
            return

        try:
            (self.root / name).unlink(missing_ok=True)
        except OSError:
            LOG.exception("Failed to evict the cached copy of %s", name)
