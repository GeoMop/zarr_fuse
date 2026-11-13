# Design: IceChunk versioning and schema-aware repository API

## Goal

Extend `zarr-fuse` with IceChunk-based dataset versioning while keeping the API simpler than direct IceChunk usage and preserving access to the complete low-level IceChunk API.

Compared with the current `zarr-fuse` implementation, the extension should provide:

* persistent versioning of Zarr datasets through IceChunk snapshots;
* atomic transactions spanning multiple Zarr/Xarray writes;
* branches and tags for named repository states;
* optimistic concurrent writes using IceChunk conflict handling;
* explicit schema versioning and schema compatibility rules above IceChunk;
* traversal and comparison of the same logical `Node` in different repository snapshots;
* minimal wrapping of IceChunk: do not duplicate its repository/session API where direct access is sufficient;
* reuse of current `zarr-fuse` storage configuration from schema files and environment variables.

The intended abstraction is:

```text
storage location
      ↓
IceChunk Storage
      ↓
IceChunk Repository
      ↓
ZFRepository
      ↓
IceChunk Session
      ↓
Node
      ↓
Zarr / Xarray
```

`ZFRepository` and `Node` should provide only zarr-fuse-specific functionality. Raw IceChunk functionality remains accessible through underlying IceChunk objects.

---

## Basic IceChunk semantics and usage

IceChunk separates physical storage from repository/version semantics:

```text
Storage
    physical backend configuration

Repository
    versioned Zarr repository stored there
```

Example:

```python
import icechunk as ic

storage = ic.s3_storage(
    bucket="my-bucket",
    prefix="datasets/my-dataset",
    endpoint_url="https://s3.example.org",
)

repo = ic.Repository.open(storage)
```

`Storage` is not a general filesystem abstraction like `fsspec`; it only defines where IceChunk repository objects are persisted.

`Repository` adds versioning operations such as branches, tags, snapshots, history and garbage collection.

A persistent dataset state is an immutable **snapshot** identified by `snapshot_id`.

```text
snapshot_id   exact immutable state
branch        mutable reference to a snapshot
tag           immutable named reference to a snapshot
```

All reads and writes use an IceChunk session.

Read:

```python
session = repo.readonly_session(branch="main")
ds = xr.open_zarr(session.store)
```

Historical read:

```python
session = repo.readonly_session(snapshot_id=snapshot_id)
ds = xr.open_zarr(session.store)
```

Write:

```python
session = repo.writable_session("main")

ds1.to_zarr(session.store, group="a", mode="a")
ds2.to_zarr(session.store, group="b", mode="a")

snapshot_id = session.commit("Update datasets")
```

`session.store` implements the Zarr v3 `Store` interface. Multiple writes through the same writable session form one transaction; `commit()` atomically creates a new snapshot and advances the branch.

If the session is abandoned without a commit, no new repository state is published. Advanced IceChunk operations such as rebase, fork/merge, branches, tags and garbage collection should remain accessible through the underlying IceChunk objects rather than being duplicated by zarr-fuse.

---

## `ZFRepository`

Introduce a thin repository wrapper class.
Conceptually:

```python
class ZFRepository:
    ic: icechunk.Repository
    schema_family: ...
    ...
```

The complete IceChunk repository remains available as:
```python
repo.ic.create_tag(...)
repo.ic.list_branches()
repo.ic.ancestry(...)
repo.ic.garbage_collect(...)
```

We provide a construction helper function:

```python
repo = zf.open_repository(schema, *options)
```

The helper should:

1. load the zarr-fuse schema;
2. resolve storage options using the existing schema/environment configuration mechanism
   and authentication.
3. construct the corresponding IceChunk `Storage`;
4. open the existing IceChunk `Repository`;
5. return a `ZFRepository`.


### Opening data

`ZFRepository` should provide the equivalent of the current `open_store()`, but return the root `Node` 
for a selected repository snapshot given by snapshot ID, branch, or tag:

```python
root = repo.node_session(branch="main")
root = repo.node_session(snapshot=snapshot_id)
root = repo.node_session(tag="release-1")

root = repo.node_session(mode="a") # branch="main by default; open a write session for the whole repository
```

This new usage schema is an explicit two stage openning alternative to existing `zf.open_store` which will 
further be exteneded to a single function node_session openning, in particular when repository ptions are given by the schema or environment.
In order to improve naming consistency we should introduce renamed `open_store` function to `open_node`, since store is part of the session 
in the IceChunk context.

---

## Schema versioning

Schema evolution and IceChunk snapshot history are separate concepts. However schema is always 
part of the store, so the DAG of schema change snapshots forms a sub DAG of the repository DAG.

We only allow schema extensions (or other backward compatible changes) within single repository, 
but a new repository could be constructed for backward incompatible schema change, while tracking 
data origin (using a simle link to original repository and possibly with more detailed provenance in future).

A schema version will have following components:
- **family** : the repository base name (e.g. hlavo_wells), name of YAML file when stored out of the repository
- **major version** : backward incompatible changes 
- **minor version** : backward compatible changes. Minor version may be any keyword string. Schema versions need not to form a sequence.
- **hash** : exact identity


### Compatible schema evolution

* addition of nodes;
* addition of variables;
* addition of metadata;
* changes in descriptions
* backward compatible change in types
* other explicitly supported additive changes.


The serialized schema itself may continue to be stored in the snapshot, we should extend 
serialization by the version info. Partial schema comparison must be implemented.


### Incompatible schema evolution

Backward-incompatible schema changes should normally start a new repository/schema major version.

Examples may include:

* incompatible dimension changes;
* incompatible coordinate semantics;
* change in naming
* change in units
* incompatible dtype changes;
* removal or semantic replacement of variables;
* changes requiring reinterpretation of existing data;
* potentially incompatible chunking/storage-layout changes.

Global repository operations should be implemented to e.g. copy whole node from one repo to other +
repository structure modification operations. This will consistently transform data and schema.


### Repository schema operations

`ZFRepository` should provide zarr-fuse-specific schema methods such as:

- `repo.schemas()` List distinct schemas occurring in the repository.
- `repo.schema_at(snapshot)`  Return the schema of a particular snapshot.
- `repo.compare_schemas(schema_a, schema_b)` Return a structured schema difference in terms of modif. operations.
  The result class should have:
  - `comparable` flag
  - `less` flag (A < B)
  - optional list of modif operations extending A to B

The comparison result should describe actual structural differences rather than return only a boolean.


### Schema versions versus branches/tags

Schema identities are independent of branches or tags, but we will create implicit tags for snapshot with 
new schema introduction, e.g. tag names `schema/1.2`
Such a tag means "this snapshot is the canonical introduction of schema 1.2".
It does not mean that **every** descendant snapshot uses schema 1.2, e.g. when we merge it with branch using 1.2_alternative. 

However automatic merging of schemas should not be allowed, that should be done explicitly 
through appropriate global repo operation.

---

## `Node`

Do not introduce a separate public `ZFSession` abstraction unless later found necessary.

A `Node` however contains the IceChunk session as attribute:

```python
class Node:
    repo: ZFRepository
    session: icechunk.Session
    path: str
    schema: NodeSchema
```

Conceptually:

```text
logical Node identity
    repository + path

particular Node view
    repository + path + session
```

All child nodes obtained from the same root share the same IceChunk session:

```python
root = repo.open(branch="main")

a = root["measurements"]
b = a["pressure"]

assert a.session is root.session
assert b.session is root.session
```

`Node` therefore remains the zarr-fuse accessor/iterator over the logical data tree, while its session determines the repository state being viewed.

### Key Node functionality

Existing tree/data functionality remains:

```python
node["child"]
node.children()
node.dataset
node.schema
node.path
```


A writes must be followed by explicit commit of changes:

```python
root = repo.open(mode="a")
root["measurements"].update(ds1)
root["simulation"].update(ds2)
snapshot_root_node = root.commit("Update measurements and simulation")
# root == snapshot_root_node HERE
```

`Node.commit()` is a convenience delegation to `node.session.commit(...)`.
for the sake of backward compatibility and IceChunk session behavior. The original `root` node
become read-only but equivalent to the resulting snapshot. The returned node will however be again writable.

All nodes derived from the same root share the single common session. commit (and discard) could only be issued from the root 
node.

Advanced transaction functionality remains available through:

```python
node.session.rebase(...)
node.session.fork(...)
node.session.merge(...)
```

---

## Traversing repository states through a Node

A `Node` should allow opening the same logical path at another repository state:

```python
node = repo.open(branch="main")["measurements/pressure"]

old = node.at(snapshot=old_snapshot)
release = node.at(tag="release-1")
dev = node.at(branch="dev")
```

`at()` returns a new `Node` with same repository, same logical path, different read-only IceChunk session.

It should not mutate the original Node/session.

For a writable Node, historical traversal will produce read-only node, which however, could form an equivalent writable node 
as a new object.


### Node revisions

We can possibly implement `node-revisions()` to get all snapshosts with the node.

---

## Usage primer

### Open repository

```python
repo = zf.open_repository("schema.yaml")
```

### Read current `main`

```python
root = repo.open(branch="main")

ds = root["measurements"].dataset
```

### Read exact snapshot

```python
root = repo.open(snapshot=snapshot_id)

ds = root["measurements"].dataset
```

### Write transaction

```python
root = repo.open(
    branch="main",
    mode="a",
)

root["measurements"].update(measurements)
root["simulation"].update(simulation)

snapshot = root.commit(
    "Import measurements and simulation"
)
```

### Navigate the same logical node across versions

```python
pressure = repo.open(branch="main")["measurements/pressure"]

old_pressure = pressure.at(snapshot=old_snapshot)

diff = old_pressure.compare(pressure)
```

### Inspect revisions of a logical node

```python
for revision in pressure.revisions(ref="main"):
    print(
        revision.snapshot,
        revision.schema,
    )
```

### Use low-level IceChunk API

```python
repo.ic.create_tag(
    "release-1",
    snapshot,
)

pressure.session.status()
```

---

## Open questions

### Schema identity and storage

* Exact representation of schema identity:

  * `family + major + minor + hash`? 
  AGENT: this is sufficient, however family.major.minor must be unique as well as the hash.
  
* Should the complete schema remain duplicated in every snapshot through `__structure__`, or should snapshots contain an identifier plus some repository-level schema registry?
  AGENT: Idealy store it in an own chunk, so it will be stored unce unless changed, that is kind of JSON/YAML file versioning. What IceChunk supports? How it deals with attributes?
  
* What should be authoritative if schema information stored in snapshot attributes and repository-level metadata disagree?
  AGENT: We should prevent this at least when the zarr-fuse level operations are used. We can not exclude this if someone uses IceChunk layer dirrectly. 
  
  
* Should schema hashes be based on canonical serialized YAML/JSON representation?
  AGENT: I'm not sure, hashing the Python representation of the schema would be better as we can first compute the hash and then serialize together with metadata.

### Compatibility policy

* Exact rules defining compatible versus incompatible schema changes.
* Is adding a coordinate always compatible?
* Is adding a required variable compatible?
* Which metadata changes are semantically relevant?
* Is chunking part of the logical schema or only a storage property?
* Are unit changes compatible when physically convertible?
* Should variable/node rename have explicit migration semantics?
* Do we need more categories than `IDENTICAL / COMPATIBLE / INCOMPATIBLE`, e.g. backward versus forward compatibility?

### Schema version assignment

* Are minor schema versions assigned explicitly by the schema author?
* Should zarr-fuse automatically detect a new schema hash and require an explicit new minor version?
* Should introducing a new schema automatically create a tag such as `schema/1.3`?
* Can several branches legitimately introduce different compatible schema revisions from the same parent?
* How should schema revision ordering be represented when schema evolution itself becomes branching?

### Repository boundaries and migration

* Precisely which incompatible changes require a new IceChunk repository?
* Should zarr-fuse provide an explicit repository migration operation?
* How are links maintained between major-version repositories?
* Should a new repository record its source repository/snapshot as provenance?

### `ZFRepository` API

* Exact naming:

  * `open_repository()`
  * `repo.open()`
  * compatibility of current `open_store()`.
* Should `repo.open()` default to `branch="main"`?
* Should the branch/tag/snapshot selection use separate keyword arguments or a single reference object?
* Should `repo.schemas()` scan repository history or rely on a maintained schema index/cache?

### `Node` and session lifetime

* Should every `Node` expose `.session` publicly, or should it be named `.ic_session` to make the abstraction boundary explicit?
* Should `Node.commit()` be available on every descendant Node or only on the root Node?
* Should `Node` expose convenience `discard()` and IceChunk `flush()`, or leave these as `node.session.*` operations?
* What guarantees are needed for closing/abandoning writable Nodes with uncommitted changes?
* Should context-manager usage be supported?

Example:

```python
with repo.open(branch="main", mode="a") as root:
    root["foo"].update(ds)
```

and if so, should exit commit automatically or require explicit `commit()`?

### Historical Node traversal

* Exact semantics and return type of `node.revisions()`.
* Should revisions be restricted by default to ancestors of the Node's current snapshot?
* How should repository-wide DAG traversal work across multiple branch/tag heads?
* How efficiently can we determine whether a particular logical node exists in a snapshot?
* Should revisions with unchanged Node content be included, or only snapshots in which that Node actually changed?
* Do we need both:

  * `node.revisions()` = states where it exists;
  * `node.changes()` = states where its contents/schema changed?
* How should comparisons behave across compatible but different schema minor versions?

### IceChunk interaction

* Preferred default conflict/rebase policy for concurrent writes.
* Whether zarr-fuse should configure automatic `UseOurs` behavior for overlapping chunk updates or leave this explicit.
* Whether IceChunk detached `flush()` snapshots have a useful zarr-fuse-level use case.
* Garbage-collection policy for abandoned/unreferenced transaction objects and expired snapshots.
* Whether schema introduction tags should be protected from expiration/GC by policy.
