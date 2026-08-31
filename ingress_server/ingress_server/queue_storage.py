"""
Storage of the payload queue, backed by an fsspec filesystem.

The queue root is either a local directory or an ``s3://bucket/prefix`` URL.
The filesystem instance built by `_make_filesystem` is the only
backend-specific part, all queue logic is shared.

Layout
------
Items live in one of the `accepted` / `success` / `failed` queues and are
addressed by a `FileRef`, a POSIX-style key relative to the queue root:
``"<queue>/<item name>"``. The item name is flat and built by the producer
(`io.files.save_data`); refs with a nested name are still handled, so a queue
written by the previous per-endpoint directory layout keeps working.
Every payload has a metadata sidecar stored under ``ref + ".meta.json"``.

Consistency
-----------
S3 provides neither an atomic rename nor a multi-object transaction, so the
payload object alone decides where an item is, and the two object operations
are ordered so that an error in between leaves a recoverable state:

- `put_item` writes the metadata first and the payload second. Listing yields
  payloads only, hence a listed payload always has its metadata. An
  interrupted put leaves an orphaned sidecar that no listing ever returns.
- `move` moves the payload first and the metadata second. An interrupted move
  leaves the item counted as moved, again with an orphaned sidecar in the
  source queue. The opposite order must not be used: it would strip the
  metadata of an item still listed as accepted, and the worker would keep
  failing on it forever.

Writes are plain `pipe_file` calls, i.e. atomic on S3 (a PUT either lands or
does not), but not on a local filesystem. A payload torn by a crash mid-write
fails during extraction and ends up in `failed`.

NOTE: this module must not import anything from the ingress_server package
(app_config imports it, while io.* modules import app_config).
"""
import os
import logging
import posixpath

from pathlib import Path
from typing import NewType

import fsspec

LOG = logging.getLogger(__name__)

FileRef = NewType("FileRef", str)
"""Reference of a queue item: the ``"<queue>/<item name>"`` key of its payload."""

META_SUFFIX = ".meta.json"
TMP_SUFFIX = ".tmp"
LAYOUT_MARKER = ".queue_layout"

ACCEPTED = "accepted"
SUCCESS = "success"
FAILED = "failed"
QUEUE_NAMES = (ACCEPTED, SUCCESS, FAILED)

S3_SCHEME = "s3://"
S3_ENV_VARS = ("ZF_S3_ENDPOINT_URL", "ZF_S3_ACCESS_KEY", "ZF_S3_SECRET_KEY")


def _make_filesystem(url: str) -> tuple[fsspec.AbstractFileSystem, str]:
    """Create the filesystem for a queue URL; return it together with the queue root path."""
    # The scheme check must happen on the raw string: Path("s3://b/p") would mangle the URL.
    if not url.startswith(S3_SCHEME):
        # auto_mkdir lets pipe_file and mv create the queue directories on
        # demand, the way an object store does implicitly.
        return fsspec.filesystem("file", auto_mkdir=True), str(Path(url).resolve())

    missing = [name for name in S3_ENV_VARS if not os.getenv(name)]
    if missing:
        raise ValueError(
            f"S3 queue storage requires environment variables: {', '.join(missing)}"
        )

    # Mirrors zarr_fuse._s3_options (CESNET/Ceph checksum compatibility), but
    # synchronous and outside the fsspec instance cache so it never collides
    # with the async instance used for the zarr store.
    filesystem = fsspec.filesystem(
        "s3",
        key=os.environ["ZF_S3_ACCESS_KEY"],
        secret=os.environ["ZF_S3_SECRET_KEY"],
        endpoint_url=os.environ["ZF_S3_ENDPOINT_URL"],
        listings_expiry_time=1,
        max_paths=0,
        skip_instance_cache=True,
        config_kwargs={
            "request_checksum_calculation": "when_required",
            "response_checksum_validation": "when_required",
        },
    )
    return filesystem, url[len(S3_SCHEME):].rstrip("/")


class QueueStorage:
    """
    The `accepted` / `success` / `failed` queues of received payloads.

    A single consumer is assumed; the queues carry no locking.
    """

    def __init__(self, url: str):
        self.fs, self.root = _make_filesystem(url)
        self.url = self.fs.unstrip_protocol(self.root)

    @staticmethod
    def meta_ref(ref: FileRef) -> FileRef:
        """Ref of the metadata sidecar of a payload."""
        return FileRef(ref + META_SUFFIX)

    @staticmethod
    def dest_ref(ref: FileRef, dest_queue: str) -> FileRef:
        """Ref the given accepted item gets once moved to a terminal queue."""
        queue_name, _, name = ref.partition("/")
        if queue_name != ACCEPTED or not name:
            raise ValueError(f"Queue item ref is not under '{ACCEPTED}/': {ref}")
        return FileRef(f"{dest_queue}/{name}")

    def ensure_layout(self) -> None:
        """
        Prepare the queue directories and fail fast on a misconfigured backend.

        The marker object also verifies the credentials and the write
        permissions; S3 has no real directories to create.
        """
        for queue_name in QUEUE_NAMES:
            self._write(f"{queue_name}/{LAYOUT_MARKER}", b"")

    def put_item(self, name: str, payload: bytes, meta: bytes) -> FileRef:
        """Store a new item under ``accepted/<name>``; return its ref."""
        ref = FileRef(f"{ACCEPTED}/{name}")

        # Metadata first, see the module docstring.
        self._write(self.meta_ref(ref), meta)
        self._write(ref, payload)

        return ref

    def list_accepted(self) -> list[FileRef]:
        """
        Payload refs in the accepted queue, sorted by the item name, i.e. by
        the endpoint first and by the receipt time second.
        """
        names = [
            name
            for name in self._item_names(ACCEPTED)
            if not name.endswith((META_SUFFIX, TMP_SUFFIX))
        ]
        return [FileRef(f"{ACCEPTED}/{name}") for name in sorted(names)]

    def read_bytes(self, ref: FileRef) -> bytes:
        return self.fs.cat_file(self._abs(ref))

    def read_meta_text(self, ref: FileRef) -> str:
        return self.fs.cat_file(self._abs(self.meta_ref(ref))).decode("utf-8")

    def move(self, ref: FileRef, dest_queue: str) -> FileRef:
        """Move a payload and its metadata from accepted/ to success/ or failed/."""
        dest = self.dest_ref(ref, dest_queue)

        # Payload first, see the module docstring.
        self.fs.mv(self._abs(ref), self._abs(dest))

        meta_src = self._abs(self.meta_ref(ref))
        if self.fs.exists(meta_src):
            try:
                self.fs.mv(meta_src, self._abs(self.meta_ref(dest)))
            except Exception:
                # The payload has moved already, so the item is accounted for;
                # only an orphaned sidecar stays behind in the source queue.
                LOG.exception("Failed to move metadata of %s, sidecar left behind", ref)

        return dest

    def recover_failed(self) -> None:
        """Move all items from failed/ back to accepted/."""
        # Metadata first, so that a payload listed in accepted/ always has its metadata.
        names = sorted(self._item_names(FAILED), key=lambda name: not name.endswith(META_SUFFIX))

        for name in names:
            self.fs.mv(self._abs(f"{FAILED}/{name}"), self._abs(f"{ACCEPTED}/{name}"))

    def _abs(self, ref: str) -> str:
        return f"{self.root}/{ref}"

    def _write(self, ref: str, data: bytes) -> None:
        try:
            self.fs.pipe_file(self._abs(ref), data)
        except Exception:
            LOG.exception("Failed to write %s to the queue %s", ref, self.url)
            raise

    def _item_names(self, queue_name: str) -> list[str]:
        """Names of the objects in a queue, payloads and metadata sidecars alike."""
        self.fs.invalidate_cache()

        base = self._abs(queue_name)
        names = [path[len(base) + 1:] for path in self.fs.find(base)]

        return [name for name in names if posixpath.basename(name) != LAYOUT_MARKER]
