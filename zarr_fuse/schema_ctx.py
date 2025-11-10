"""
Schema context classes provides association of various zarr_fuse objects
with their location in the schema tree in order to provide consistent and
informed error messages.
"""

from typing import *
import attrs
from . import logger as zf_logger

class RaisingLogger(zf_logger.Logger):
    def error(self, exc, *args, **kwargs):
        super().error(exc, *args, **kwargs)
        if not isinstance(exc, BaseException):
            exc = RuntimeError(str(exc))
        raise exc # fallback if a plain message was passed


def default_logger():
    return RaisingLogger("default schema logger")


class SchemaErrBase:
    """
    Mixin that holds message and its origincontext for both
    the Exception and the Warning classes.
    """
    def __init__(self, message: str, ctx: 'SchemaCtx'):
        self.message = message
        self.address = ctx

    def __str__(self) -> str:
        return f"{self.message}  (at {self.address})"


class SchemaError(SchemaErrBase, Exception):
    """Raise when the config problem should be fatal."""
    pass


class SchemaWarning(SchemaErrBase, UserWarning):
    """Emit when the problem should be non-fatal."""
    pass







SchemaKey = Union[str, int]
SchemaPath = SchemaKey | List[SchemaKey]
@attrs.define(frozen=True)
class SchemaCtx:
    """
    Represents a single value in the schema file.
    Holds the source file name (or empty string for an anonymous stream)
    and a list of path components locating the value within the YAML tree.

    Path components are stored as provided (str or int) and converted to
    strings only when rendering.
    """
    addr: SchemaPath
    file: str = attrs.field(default=None, eq=False)
    logger: zf_logger.Logger = attrs.field(factory=default_logger)

    @property
    def path(self) -> str:
        """Return the path as a string."""
        return self._join(self.addr)

    @staticmethod
    def _join(addr):
        return '/'.join(map(str, addr))

    def __str__(self) -> str:
        file_repr = self.file if self.file else "<SCHEMA STREAM>"
        return f"{file_repr}:{self.path}"

    def dive(self, *path) -> "SchemaCtx":
        """Return a new SchemaAddress with an extra path component."""
        addr = self.addr + list(path)
        return SchemaCtx(addr, self.file, self.logger)

    def parent(self) -> "SchemaCtx":
        """Return a new SchemaAddress for the parent path."""
        if isinstance(self.addr, list) and len(self.addr) > 0:
            addr = self.addr[:-1]
        else:
            addr = []
        return SchemaCtx(addr, self.file, self.logger)

    def error(self, message: str, **kwargs) -> SchemaError:
        err = SchemaError(message, self)
        self.logger.error(err, **kwargs)
        return err

    def warning(self, message: str, **kwargs) -> SchemaWarning:
        warn = SchemaWarning(message, self)
        self.logger.warning(warn, **kwargs)
        return warn

@attrs.define
class ContextCfg:
    cfg: Dict[str, Any] | List[Any]
    schema_ctx: SchemaCtx

    def __getitem__(self, key: str| int) -> Any:
        return ContextCfg(self.cfg[key], self.schema_ctx.dive(key))

    def __setitem__(self, key: str| int, value: Any) -> None:
        self.cfg[key] = value

    def __contains__(self, item):
        return item in self.cfg

    def value(self) -> Any:
        return self.cfg

    def pop(self, key: str| int, default=None) -> Any:
        value = self.cfg.pop(key, default)
        return ContextCfg(value, self.schema_ctx.dive(key))

    def keys(self):
        return self.cfg.keys()

    def get(self, key: str| int, default=None) -> Any:
        value = self.cfg.get(key, default)
        return ContextCfg(value, self.schema_ctx.dive(key))



class AddressMixin:
    """
    Mixin for schema objects that keeps their source SchemaAddress and
    provides convenience helpers to raise errors / emit warnings bound
    to that address.

    The helpers accept an optional list of `subkeys` to append to the
    stored address. This is useful when the message refers to a nested
    attribute/key (e.g., a field inside ATTRS/VARS/COORDS).
    """
    _address: SchemaCtx  # subclasses provide this via attrs field

    def _extend_address(self, subkeys: List[Union[str, int]] = []) -> SchemaCtx:
        addr = self._address
        for k in subkeys:
            addr = addr.dive(k)
        return addr

    def error(self, message: str, subkeys: Optional[List[Union[str, int]]] = []) -> None:
        # Deprecated: use self._address.error() directly
        addr = self._extend_address(subkeys)
        addr.error(message)

    def warn(self, message: str, subkeys: Optional[List[Union[str, int]]] = [], *, stacklevel: int = 2) -> None:
        # deprecated: use self._address.warning() directly
        addr = self._extend_address(subkeys)
        addr.warning(message, stacklevel=stacklevel,)
