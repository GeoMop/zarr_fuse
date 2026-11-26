import hashlib
import json
from pathlib import Path
from typing import Any, Dict

import icechunk as ic
import yaml
import zarr

from . import zarr_schema
from .zarr_storage import (
    STORE_OPTIONS,
    Node,
    ZFOptionError,
    _get_option,
    _get_schema,
    _local_store_path,
    _option_logger,
    _s3_options,
    _zarr_fuse_options,
)

SCHEMA_ARRAY = "__zf__/schema"
SCHEMA_TAG = "schema/"
# The library version, the version labels and the storage options are not part of the schema identity.
_NOT_IDENTITY_ATTRS = {'VERSION', 'SCHEMA_FAMILY', 'SCHEMA_VERSION', *STORE_OPTIONS}


def _schema_hash(structures: Dict[str, str]) -> str:
    """SHA-256 of the node schemas in their Python representation."""
    def identity(structure: str) -> dict:
        ds = yaml.safe_load(structure)
        ds['ATTRS'] = {key: value for key, value in ds.get('ATTRS', {}).items() if key not in _NOT_IDENTITY_ATTRS}
        return ds

    canonical = json.dumps({path: identity(s) for path, s in structures.items()}, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


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

    def stored_schema(self, session: ic.Session) -> Dict[str, Any]:
        """
        Schema family, version, hash and node schemas stored in the session state,
        empty before the first commit.
        """
        if SCHEMA_ARRAY not in zarr.open_group(session.store, mode='r'):
            return {}
        return json.loads(zarr.open_array(session.store, path=SCHEMA_ARRAY, mode='r')[()])

    def commit_session(self, session: ic.Session, structures: Dict[str, str], message: str,
                       metadata: Dict[str, Any] | None = None, **kwargs) -> str:
        """
        Commit the session with the schema identity in the commit metadata, kwargs go to `icechunk.Session.commit`.
        A changed schema is stored as a new schema version and its snapshot is tagged 'schema/<version>'.
        Return: the snapshot ID.
        """
        record = self.stored_schema(session)
        schema_hash = _schema_hash(structures)
        new_schema = record.get('hash') != schema_hash
        if new_schema:
            record = self._new_schema_record(record, structures, schema_hash)
            array = zarr.create_array(session.store, name=SCHEMA_ARRAY, shape=(), dtype=str, overwrite=True)
            array[()] = json.dumps(record)

        identity = {f"schema_{key}": record[key] for key in ('family', 'version', 'hash')}
        snapshot = session.commit(message, metadata={**identity, **(metadata or {})}, **kwargs)
        if new_schema:
            self.ic.create_tag(SCHEMA_TAG + record['version'], snapshot)
        return snapshot

    def _new_schema_record(self, stored: Dict[str, Any], structures: Dict[str, str],
                           schema_hash: str) -> Dict[str, Any]:
        attrs = self.schema.ds.ATTRS
        family, version = attrs.get('SCHEMA_FAMILY'), attrs.get('SCHEMA_VERSION')
        major, _, minor = str(version).partition('.')
        if not isinstance(family, str) or not isinstance(version, str) or not major.isdigit() or not minor:
            raise ValueError(
                "A changed repository schema needs SCHEMA_FAMILY and SCHEMA_VERSION '<major>.<minor>' "
                f"in the root ATTRS, got {family!r} and {version!r}."
            )
        if stored and (family != stored['family'] or major != stored['version'].partition('.')[0]):
            raise ValueError(
                f"Schema {family} {version} does not extend the repository schema {stored['family']} "
                f"{stored['version']}, other family or major version needs a new repository."
            )
        if SCHEMA_TAG + version in self.ic.list_tags():
            raise ValueError(f"The schema changed, but its version {version} is already in the repository.")
        return {'family': family, 'version': version, 'hash': schema_hash, 'nodes': structures}


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
