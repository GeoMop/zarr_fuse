"""
Storage backends for the payload queue.

Queue items are addressed by POSIX-style keys relative to the queue root,
e.g. "accepted/<endpoint>/<name>". Every payload has a metadata sidecar
stored under `key + ".meta.json"`.

Write ordering contract (both backends):
- `put_item` writes the metadata first, the payload second. Listing keys off
  payload objects, so a listed payload always has its metadata present.
- `move` moves the payload first, the metadata second (an orphaned metadata
  object in `accepted/` is invisible to listing; a payload without metadata
  can only appear in a terminal queue).

NOTE: this module must not import anything from the ingress_server package
(app_config imports it, while io.* modules import app_config).
"""
import os
import abc
import time
import uuid
import shutil
import logging
import posixpath

from pathlib import Path, PurePosixPath

LOG = logging.getLogger(__name__)

META_SUFFIX = ".meta.json"
TMP_SUFFIX = ".tmp"

ACCEPTED = "accepted"
SUCCESS = "success"
FAILED = "failed"
QUEUE_NAMES = (ACCEPTED, SUCCESS, FAILED)


def new_msg_name(suffix: str) -> str:
    ts = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    uid = uuid.uuid4().hex[:12]
    return f"{ts}_{uid}{suffix}"


class QueueStorage(abc.ABC):
    """
    Abstract queue storage; `url` is a display string for logs.
    """

    url: str

    @staticmethod
    def from_url(queue_dir_path: str) -> "QueueStorage":
        # The scheme check must happen on the raw string: Path("s3://b/p")
        # would mangle the URL.
        if queue_dir_path.startswith("s3://"):
            return S3QueueStorage(queue_dir_path)
        return LocalQueueStorage(Path(queue_dir_path).resolve())

    @staticmethod
    def basename(key: str) -> str:
        return posixpath.basename(key)

    @staticmethod
    def meta_key(key: str) -> str:
        return key + META_SUFFIX

    @staticmethod
    def dest_key(key: str, dest_queue: str) -> str:
        queue_name, _, rest = key.partition("/")
        if queue_name != ACCEPTED or not rest:
            raise ValueError(f"Queue item key is not under '{ACCEPTED}/': {key}")
        return f"{dest_queue}/{rest}"

    @abc.abstractmethod
    def ensure_layout(self) -> None:
        """Prepare the queue root; fail fast on misconfiguration."""

    @abc.abstractmethod
    def put_item(self, endpoint_name: str, name: str, payload: bytes, meta: bytes) -> str:
        """Store a new item under accepted/<endpoint_name>/<name>; return its key."""

    @abc.abstractmethod
    def list_accepted(self) -> list[str]:
        """Payload keys under accepted/, sorted by basename (receipt order)."""

    @abc.abstractmethod
    def read_bytes(self, key: str) -> bytes:
        ...

    @abc.abstractmethod
    def read_meta_text(self, key: str) -> str:
        ...

    @abc.abstractmethod
    def move(self, key: str, dest_queue: str) -> None:
        """Move payload and metadata from accepted/ to success/ or failed/."""

    @abc.abstractmethod
    def recover_failed(self) -> None:
        """Move all items from failed/ back to accepted/, preserving subtrees."""


class LocalQueueStorage(QueueStorage):

    def __init__(self, root: Path):
        self.root = root
        self.url = str(root)

    def _abs(self, key: str) -> Path:
        return self.root / PurePosixPath(key)

    def ensure_layout(self) -> None:
        for queue_name in QUEUE_NAMES:
            (self.root / queue_name).mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _atomic_write(path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + TMP_SUFFIX)
        tmp.write_bytes(data)
        os.replace(tmp, path)

    def put_item(self, endpoint_name: str, name: str, payload: bytes, meta: bytes) -> str:
        key = f"{ACCEPTED}/{endpoint_name}/{name}"

        meta_path = self._abs(self.meta_key(key))
        try:
            self._atomic_write(meta_path, meta)
        except Exception:
            LOG.exception("Failed to save metadata to %s", meta_path)
            raise

        payload_path = self._abs(key)
        try:
            self._atomic_write(payload_path, payload)
        except Exception:
            LOG.exception("Failed to save data to %s", payload_path)
            raise

        return key

    def list_accepted(self) -> list[str]:
        accepted_dir = self.root / ACCEPTED
        if not accepted_dir.exists():
            LOG.warning("Accepted directory does not exist: %s", accepted_dir)
            return []

        keys: list[str] = []
        for walk_root, _, files in os.walk(accepted_dir):
            for file_name in files:
                if file_name.endswith(META_SUFFIX) or file_name.endswith(TMP_SUFFIX):
                    continue
                path = Path(walk_root) / file_name
                keys.append(str(PurePosixPath(path.relative_to(self.root))))

        return sorted(keys, key=posixpath.basename)

    def read_bytes(self, key: str) -> bytes:
        return self._abs(key).read_bytes()

    def read_meta_text(self, key: str) -> str:
        return self._abs(self.meta_key(key)).read_text(encoding="utf-8")

    def move(self, key: str, dest_queue: str) -> None:
        dest = self.dest_key(key, dest_queue)

        dst = self._abs(dest)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(self._abs(key)), str(dst))

        meta_src = self._abs(self.meta_key(key))
        if meta_src.exists():
            shutil.move(str(meta_src), str(self._abs(self.meta_key(dest))))

    def recover_failed(self) -> None:
        self._move_tree_contents(self.root / FAILED, self.root / ACCEPTED)

    @staticmethod
    def _move_tree_contents(src: Path, dst: Path) -> None:
        if not src.exists():
            LOG.warning("Source directory does not exist: %s", src)
            return

        dst.mkdir(parents=True, exist_ok=True)
        for root, _, files in os.walk(src, topdown=False):
            root_p = Path(root)
            rel = root_p.relative_to(src)
            target_root = dst / rel
            target_root.mkdir(parents=True, exist_ok=True)

            for name in files:
                s = root_p / name
                d = target_root / name
                d.parent.mkdir(parents=True, exist_ok=True)

                try:
                    os.replace(s, d)
                except Exception as exc:
                    LOG.warning(
                        "os.replace failed for %s -> %s, falling back to copy2: %s",
                        s,
                        d,
                        exc,
                    )
                    shutil.copy2(s, d)
                    s.unlink(missing_ok=True)

            if root_p != src:
                try:
                    root_p.rmdir()
                except OSError:
                    pass


def _s3_filesystem():
    # Lazy import: the local backend must not require s3fs at import time.
    import fsspec

    missing = [
        name
        for name in ("ZF_S3_ENDPOINT_URL", "ZF_S3_ACCESS_KEY", "ZF_S3_SECRET_KEY")
        if not os.getenv(name)
    ]
    if missing:
        raise ValueError(
            f"S3 queue storage requires environment variables: {', '.join(missing)}"
        )

    # Mirrors zarr_fuse._s3_options (CESNET/Ceph checksum compatibility), but
    # synchronous and outside the fsspec instance cache so it never collides
    # with the async instance used for the zarr store.
    return fsspec.filesystem(
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


class S3QueueStorage(QueueStorage):

    def __init__(self, url: str):
        self.url = url.rstrip("/")
        self._root = self.url[len("s3://"):]
        self._fs = _s3_filesystem()

    def _abs(self, key: str) -> str:
        return f"{self._root}/{key}"

    def _rel(self, path: str) -> str:
        return path[len(self._root) + 1:]

    def ensure_layout(self) -> None:
        # S3 has no directories; write a marker object at the queue root to
        # fail fast on missing credentials or write permissions.
        self._fs.pipe_file(self._abs(".queue_layout"), b"")

    def put_item(self, endpoint_name: str, name: str, payload: bytes, meta: bytes) -> str:
        key = f"{ACCEPTED}/{endpoint_name}/{name}"

        try:
            self._fs.pipe_file(self._abs(self.meta_key(key)), meta)
        except Exception:
            LOG.exception("Failed to save metadata to %s/%s", self.url, self.meta_key(key))
            raise

        try:
            self._fs.pipe_file(self._abs(key), payload)
        except Exception:
            LOG.exception("Failed to save data to %s/%s", self.url, key)
            raise

        return key

    def list_accepted(self) -> list[str]:
        self._fs.invalidate_cache()

        keys: list[str] = []
        for path in self._fs.find(self._abs(ACCEPTED)):
            key = self._rel(path)
            if key.endswith(META_SUFFIX) or key.endswith(TMP_SUFFIX) or key.endswith("/"):
                continue
            keys.append(key)

        return sorted(keys, key=posixpath.basename)

    def read_bytes(self, key: str) -> bytes:
        return self._fs.cat_file(self._abs(key))

    def read_meta_text(self, key: str) -> str:
        return self._fs.cat_file(self._abs(self.meta_key(key))).decode("utf-8")

    def move(self, key: str, dest_queue: str) -> None:
        dest = self.dest_key(key, dest_queue)
        self._fs.mv(self._abs(key), self._abs(dest))

        meta_abs = self._abs(self.meta_key(key))
        if self._fs.exists(meta_abs):
            self._fs.mv(meta_abs, self._abs(self.meta_key(dest)))

    def recover_failed(self) -> None:
        self._fs.invalidate_cache()

        recovered = [self._rel(path) for path in self._fs.find(self._abs(FAILED))]
        # Metadata first, so a payload listed in accepted/ always has its metadata.
        for key in sorted(recovered, key=lambda k: not k.endswith(META_SUFFIX)):
            dest = f"{ACCEPTED}/{key.partition('/')[2]}"
            self._fs.mv(self._abs(key), self._abs(dest))
