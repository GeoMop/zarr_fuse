from pathlib import Path
from typing import Any, Dict

import icechunk as ic

from . import zarr_schema
from .zarr_storage import (
    Node,
    ZFOptionError,
    _get_option,
    _get_schema,
    _local_store_path,
    _option_logger,
    _s3_options,
    _zarr_fuse_options,
)


def _icechunk_storage(options: Dict[str, Any]) -> ic.Storage:
    store_url = _get_option(options, 'STORE_URL')
    if store_url.startswith('s3://'):
        bucket, _, prefix = store_url[len('s3://'):].partition('/')
        s3_options = _s3_options(options)
        return ic.s3_storage(
            bucket=bucket,
            prefix=prefix,
            endpoint_url=s3_options['endpoint_url'],
            access_key_id=s3_options['key'],
            secret_access_key=s3_options['secret'],
            # Path style addressing works also for S3 compatible stores without bucket DNS names.
            force_path_style=True,
        )
    if store_url.startswith('zip://'):
        raise ZFOptionError(f"IceChunk repository can not be stored in a zip file: {store_url}")
    return ic.local_filesystem_storage(str(_local_store_path(options)))


class ZFRepository:
    """
    IceChunk repository of a zarr-fuse schema.
    The IceChunk Repository is available as `ic` for the operations not provided here.
    """

    def __init__(self, ic_repo: ic.Repository, schema: zarr_schema.NodeSchema, options: Dict[str, Any]):
        self.ic = ic_repo
        self.schema = schema
        self.options = options

    def open(self, branch: str | None = None, *, tag: str | None = None, snapshot: str | None = None,
             mode: str = 'r') -> Node:
        """
        Root Node of the repository state given by a branch, a tag or a snapshot ID, the branch 'main' by default.
        The mode 'a' opens a writable session of the branch, `Node.commit` publishes its changes.
        """
        if mode == 'r':
            if tag is None and snapshot is None:
                branch = branch or 'main'
            session = self.ic.readonly_session(branch, tag=tag, snapshot_id=snapshot)
        else:
            if tag is not None or snapshot is not None:
                raise ValueError("Only a branch can be opened for writing.")
            session = self.ic.writable_session(branch or 'main')
        return Node("", session.store, new_schema=self.schema, mode=mode,
                    logger=_option_logger(self.options), repo=self, session=session)


def open_repository(schema: zarr_schema.NodeSchema | Path | str | dict, **kwargs) -> ZFRepository:
    """
    Open the IceChunk repository of the schema, it is created unless the MODE option is 'r'.
    The STORE_URL and the other options are resolved as in `open_node`.
    """
    node_schema = _get_schema(schema)
    options = _zarr_fuse_options(node_schema, **kwargs)
    try:
        storage = _icechunk_storage(options)
    except ZFOptionError as e:
        raise ZFOptionError(f"{str(e)}. Opening repository for schema {schema}.")

    if options.get('MODE', 'a') == 'r':
        ic_repo = ic.Repository.open(storage)
    else:
        ic_repo = ic.Repository.open_or_create(storage)
    return ZFRepository(ic_repo, node_schema, options)
