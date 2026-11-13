# ZFWorkspace preliminary design

Based on the chat: https://chatgpt.com/share/6a8f08ff-5588-83ed-a1a4-5d2a03dbaa88

## 1. Goal

`ZFWorkspace` extends `zarr_fuse` from a single versioned Zarr/IceChunk repository to a logical workspace composed of several weakly connected IceChunk repositories, typically because access permissions can only be enforced reliably at bucket level.

The main requirements are:

- preserve the existing `Node`-based `zarr_fuse` data model;
- use IceChunk for Zarr versioning and transactions;
- allow one logical workspace to span several buckets / IceChunk repositories;
- provide one persistent version reference for the complete application state;
- support persistent cross-references between repositories;
- support versioned arbitrary files outside Zarr through `fsspec`;
- keep repository-level permissions independent;
- support concurrent writers without long-held locks;
- keep the first implementation minimal and avoid duplicating IceChunk APIs.

The intended mental model is a **Git repository with submodules**:

- each member IceChunk repository is analogous to a submodule;
- the workspace repository is analogous to the Git superproject;
- a workspace snapshot stores the exact snapshot ID of every member repository together with application state;
- the workspace snapshot ID is the canonical persistent reference to the complete workspace state.

---

## 2. IceChunk semantics used by the design

IceChunk separates physical storage from repository/version semantics:

```text
Storage
    physical backend configuration

Repository
    versioned Zarr repository stored there

Session
    selected repository state / writable transaction

session.store
    Zarr v3 Store used by Zarr/Xarray
```

Important semantics used by `zarr_fuse`:

- one IceChunk repository uses one IceChunk `Storage`;
- for object storage, the storage is tied to one bucket/container plus prefix;
- a repository therefore does not naturally span several independently permissioned buckets;
- a snapshot is an immutable repository state identified by `snapshot_id`;
- a branch is a mutable reference to a snapshot;
- a tag is an immutable named reference to a snapshot;
- a writable session accumulates changes and `commit()` publishes a snapshot and advances its branch;
- after successful `commit()`, the original session becomes read-only and represents the committed snapshot;
- a new writable session is required for further writes;
- multiple independent writable sessions are allowed and IceChunk resolves branch concurrency optimistically;
- IceChunk sessions/repositories remain directly accessible for advanced operations; `zarr_fuse` should not mirror the entire IceChunk API.

`flush()` is deliberately excluded from the initial `ZFWorkspace` design. Workspace coordination is based on normal repository commits and explicit branch/snapshot references.

---

## 3. Main architecture

```text
ZFWorkspace
│
├── workspace IceChunk repository
│      ├── workspace application state
│      ├── member repository catalogue
│      └── member repository snapshot map
│
├── ZFRepository A  -> bucket A
├── ZFRepository B  -> bucket B
├── ZFRepository C  -> bucket C
└── ...
```

The workspace repository is itself an IceChunk repository, but its main role is to hold the **global logical state**.

A workspace snapshot contains approximately:

```text
workspace snapshot W17
│
├── repository A -> snapshot A42
├── repository B -> snapshot B11
├── repository C -> snapshot C93
└── application state
```

Therefore:

```text
workspace snapshot ID
    = canonical persistent reference to the whole application state
```

A workspace snapshot is authoritative. Member repository branch heads are implementation details and must not be used to reconstruct a workspace state.

---

## 4. Repository identity and cross-references

Each member repository should have a stable logical repository ID independent of physical storage configuration.

Conceptually:

```python
RepositoryRef(
    repository_id="simulation",
    snapshot_id="...",
    path="/results/pressure",
)
```

The workspace resolves `repository_id` to physical storage configuration and credentials at runtime.

Persistent references should not contain credentials.

Two reference styles may eventually be useful:

### Workspace-relative reference

```python
RepositoryRef(
    repository_id="simulation",
    path="/results/pressure",
)
```

The referenced snapshot is resolved from the surrounding workspace snapshot.

This is preferred for structural links inside one workspace state.

### Explicitly pinned reference

```python
RepositoryRef(
    repository_id="simulation",
    snapshot_id="...",
    path="/results/pressure",
)
```

This is useful for provenance or references that must remain fixed independently of the workspace in which they are viewed.

---

## 5. Node hierarchy

The single current `Node` class is likely too broad once workspace nodes, repository roots, ordinary repository children and filesystem-prefix nodes have different semantics.

A shallow hierarchy is proposed:

```text
Node
│
├── WorkspaceRoot
│
└── RepositoryNode
     │
     ├── RepositoryRoot
     │
     └── RepositoryChild
            │
            └── FilePrefix
```

### 5.1 `Node`

Common logical-tree behavior only:

- `name`
- `parent`
- path handling
- `root`
- `__getitem__()`
- `items()`

The base class should not assume the existence of an IceChunk session or Zarr store.

### 5.2 `WorkspaceRoot`

Represents one workspace state / workspace transaction.

It has no public IceChunk `session` because its `commit()` coordinates several repository sessions rather than delegating to one session.

It maintains conceptually:

```python
base_workspace_snapshot
base_repository_snapshots
repository_roots
pending_repository_snapshots
transaction_id
read_set
write_set
```

### 5.3 `RepositoryNode`

Adds repository-local storage semantics:

```python
repository
session
store
```

### 5.4 `RepositoryRoot`

Owns one IceChunk writable/read-only session.

Only `RepositoryRoot` may call high-level `commit()`.

Its descendants share the same session.

### 5.5 `RepositoryChild`

Represents a normal Zarr subtree.

Calling:

```python
child.commit(...)
```

must raise an explicit transaction-scope error because commit applies to the complete shared repository session, not to the child subtree.

### 5.6 `FilePrefix`

Represents a logical managed-file prefix and corresponding file-manifest subtree.

It may expose operations such as:

```python
put(...)
open(...)
files(...)
checkout(...)
```

but transaction semantics remain those of its `RepositoryRoot`.

---

## 6. `RepositoryRoot.commit()` behavior

`RepositoryRoot.commit()` delegates to its IceChunk session.

Example:

```python
root = repo.open(branch="main", mode="a")
root["data"].update(ds)
new_root = root.commit("update")
```

Postconditions:

```text
old root
    read-only
    exact committed snapshot

new_root
    fresh writable session
    same logical repository/path/branch
```

If the IceChunk commit succeeds but opening the fresh writable session fails, the API must distinguish this from commit failure. The committed snapshot ID must remain recoverable from the exception/result.

Inside a `ZFWorkspace` transaction, repository commits use transaction-private branches rather than shared repository `main`.

---

## 7. Read-only opening must be side-effect free

Historical or normal read-only opening must never reconcile or mutate storage.

This is particularly important because the current `Node` construction performs schema/storage consistency work.

The desired invariant is:

```text
repo.open(..., mode="r")
    strictly reads existing state

repo.open(..., mode="a")
    explicitly opens writable state
```

Historical snapshot access must be strictly read-only and side-effect free.

---

## 8. Schema identity

Operational storage configuration must not contribute to schema identity.

The schema identity should cover only logical schema structure.

Conceptually:

```text
schema hash
    canonical identity

schema version
    optional arbitrary human-readable identifier
    unique within the repository/workspace schema namespace
```

For example:

```text
family:  hlavo
major:   2
version: experimental-grid-2026
hash:    <canonical schema hash>
```

The version string does not imply ordering and may be any unique string.

The schema hash remains canonical.

Operational values such as:

- storage URL;
- S3 endpoint;
- credentials;
- logger configuration;
- work directory;

must be excluded from canonical schema serialization/hash.

Schema catalogue traversal and repository-wide schema-history APIs are deferred from the minimal implementation.

---

## 9. Managed arbitrary files

The workspace must support files outside Zarr/IceChunk, for example provenance files, meshes or external software inputs/outputs.

The design intentionally avoids a second version-control system such as DVC or lakeFS.

Instead:

- file bytes are stored through `fsspec`;
- every file version is immutable;
- physical version identity is derived from file content;
- IceChunk snapshots version the manifest references to these immutable files.

### 9.1 Logical and physical names

Preserve user-defined directory/prefix structure as much as possible.

Logical name:

```text
experiment_A/mesh/domain.vtk
```

Physical immutable name:

```text
experiment_A/mesh/domain_<compact-content-id>.vtk
```

The compact ID is derived from a cryptographic content hash and encoded using a compact Base62-like alphabet (`0-9A-Za-z`).

The original logical path is stored exactly in the manifest and is restored on checkout.

If required by filesystem filename-length limits, only the physical basename may be shortened. The logical original filename remains unchanged in metadata.

Case-sensitive storage is assumed for the local/debug backend. On Windows WSL2 storage is preferred; on macOS a case-sensitive APFS volume/image is preferred.

### 9.2 File manifest

Managed file metadata is stored in an IceChunk/Zarr manifest repository/tree.

The manifest tree mirrors the logical file prefix hierarchy:

```text
managed files

experiment_A/
    input/
        mesh_<hash>.vtk
        parameters_<hash>.yaml
    output/
        result_<hash>.csv

manifest Zarr hierarchy

/__zf_files__/
    experiment_A/
        input/
            manifest dataset
        output/
            manifest dataset
```

There is one manifest dataset per logical prefix rather than one global JSON manifest.

A minimal manifest record contains approximately:

```text
name
    exact original basename

stored_name
    exact physical immutable basename

hash
    complete cryptographic content hash

size
    byte size
```

The exact physical stored name is persisted rather than always reconstructed from the logical name and hash. This allows future changes to the physical naming algorithm without invalidating historical manifests.

### 9.3 String representation

The manifest requires robust arbitrary UTF-8 strings.

Variable-length UTF-8 handling should first be implemented and tested directly at the Zarr layer rather than depending on fragile Xarray string round-tripping.

Tests should include:

- ASCII;
- Czech/European Unicode;
- non-Latin Unicode;
- emoji;
- empty strings;
- long strings.

### 9.4 Existing physical object verification

A physical object whose expected hashed/versioned name already exists must not simply be trusted.

On reuse:

```text
object does not exist
    -> upload

object exists
    -> verify size/hash
       identical -> reuse
       mismatch  -> corruption error
```

Managed version objects are immutable and must never be silently overwritten.

### 9.5 Checkout

Checkout restores logical/original filenames for use by external software.

Files should be streamed to a temporary local path while hashing, then atomically renamed only after successful verification.

Logical paths must be normalized and validated:

- relative POSIX-style namespace internally;
- no `..` traversal;
- no absolute paths;
- no NUL;
- detect target-filesystem case collisions and invalid platform-specific names before partial checkout.

### 9.6 Orphans and garbage collection

External file upload happens before the manifest/workspace commit.

If a later commit fails, an unreferenced immutable file may remain.

This is acceptable and intentionally preferred over attempting distributed rollback.

Future garbage collection may delete objects that are:

- unreferenced by all retained workspace states;
- sufficiently old to avoid deleting objects belonging to still-active transactions.

A grace period is therefore required.

---

## 10. File storage and IceChunk layout

Clear namespace separation is recommended, although the file and IceChunk stores may share a bucket.

Conceptually:

```text
bucket/
    __icechunk__/
        IceChunk repository internals

    experiment_A/
        managed files

    results/
        managed files
```

Managed-file traversal must never descend into the reserved IceChunk repository prefix.

The manifest itself defines which files are managed. Arbitrary files inside the IceChunk internal prefix are forbidden.

A workspace may use a separate member IceChunk repository for managed file manifests if bucket-level permissions require it.

---

## 11. Workspace versioning model

`ZFWorkspace` provides one global history over all member repositories.

A workspace branch behaves like a Git superproject branch.

For example:

```text
workspace/main

W10
    A -> A4
    B -> B7

W11
    A -> A5
    B -> B7

W12
    A -> A5
    B -> B9
```

Persistent application links need only the workspace snapshot ID:

```text
workspace + W12
```

Explicit workspace tags are recommended for permanent named states:

```text
release-2026-08
published-model
workflow-run-1234
```

The workspace snapshot ID remains the canonical identity; tags are human-readable immutable aliases.

Public permanent tags need normally exist only in the workspace repository. Member repository snapshots are retained because workspace states reference them.

---

## 12. Workspace transaction branches

A critical concurrency rule is:

> Workspace-managed writes must never directly use a shared member-repository branch while the transaction is open.

Each writable `ZFWorkspace` transaction gets a unique transaction ID, for example:

```text
TA
TB
```

When transaction `TA` first writes member repository X, create a private branch from the exact X snapshot recorded in the base workspace state:

```text
repo X:
    __zf_tx/TA
```

Likewise for every changed repository.

Example starting from:

```text
W0:
    X -> x0
    Y -> y0
```

parallel workspace transactions prepare:

```text
repo X:

       xA  __zf_tx/TA
      /
    x0
      \
       xB  __zf_tx/TB

repo Y:

       yA  __zf_tx/TA
      /
    y0
      \
       yB  __zf_tx/TB
```

This prevents one transaction from accidentally rebasing onto unpublished changes from another transaction.

---

## 13. Workspace commit protocol

Workspace commit is a two-stage process:

### Stage 1: prepare member repository snapshots

Long-running and lock-free.

For every modified member repository:

- write through its transaction-private branch/session;
- commit the member IceChunk session;
- obtain the immutable member snapshot ID;
- record it as a pending workspace repository state.

Explicit `RepositoryRoot.commit()` may happen before `WorkspaceRoot.commit()`. Such a committed member snapshot remains pending until the workspace is published.

### Stage 2: short workspace publication

`WorkspaceRoot.commit()` performs a short serialized publication phase.

Conceptually:

```text
1. acquire workspace publication lock

2. read current workspace branch HEAD

3. validate transaction against its base workspace state

4. integrate changed member transaction snapshots
   into member canonical branches

5. commit workspace repository with:
       resulting member snapshot map
       application state

6. release workspace publication lock
```

Large data writes and member repository commits happen before this lock, so no long-running distributed lock is required.

The workspace publication lock serializes only branch-pointer integration and the small workspace commit.

---

## 14. Member repository integration

For the initial implementation, repository integration during workspace commit should be **conditional fast-forward only**, not a general data merge.

Suppose transaction A started member repository X from `x0` and prepared `xA`:

```text
x0 -> xA
```

If canonical X `main` still points to `x0`, publication may fast-forward:

```text
X/main: x0 -> xA
```

If `X/main` no longer points to the transaction base, then another workspace transaction has changed this repository and the transaction conflicts.

No automatic repository-level three-way data merge is attempted in the MVP.

This keeps conflict semantics conservative and predictable.

---

## 15. Concurrent workspace commits

### 15.1 Crossed-write example

Initial workspace:

```text
W0:
    X -> x0
    Y -> y0
```

Transaction A prepares:

```text
X -> xA
Y -> yA
```

Transaction B prepares:

```text
X -> xB
Y -> yB
```

A acquires the workspace publication lock first:

```text
X/main: x0 -> xA
Y/main: y0 -> yA
workspace/main: W0 -> WA

WA:
    X -> xA
    Y -> yA
```

B then validates against its base W0 and sees that both X and Y changed.

Result:

```text
WorkspaceConflictError
```

B publishes nothing. Its prepared snapshots remain available for retry/recovery/GC.

### 15.2 Disjoint writes

A modifies only X and B modifies only Y.

A publishes:

```text
WA:
    X -> xA
    Y -> y0
```

B then validates:

- X changed, but B did not write X, so B adopts current `xA`;
- Y is still at B's base `y0`, so B may publish `yB`.

Result:

```text
WB:
    X -> xA
    Y -> yB
```

This is equivalent to serial execution A then B.

### 15.3 Repository-level merge rule

Per member repository:

```text
ours == base
    -> take current

current == base
    -> take ours

ours == current
    -> take ours

otherwise
    -> conflict
```

For repositories written by the transaction, a changed current snapshot generally produces a conflict in the MVP.

---

## 16. Read dependencies and serializability

Write-set conflict detection alone gives only snapshot-isolation-like behavior.

Example:

```text
A reads Y and writes X
B reads X and writes Y
```

Their write sets are disjoint but both computations may depend on stale inputs.

Therefore a writable `WorkspaceRoot` should track repository-level read and write sets.

Conservative initial rule:

> Any member repository opened through a writable workspace counts as read.

At workspace commit, if a repository in the transaction read set has changed since the base workspace snapshot, publication should fail.

Conceptual validation:

```text
for every repository in read_set or write_set:
    if current_snapshot != base_snapshot:
        if repository was read or written by this transaction:
            conflict
```

This is intentionally repository-granular rather than object/chunk-granular.

Advanced direct IceChunk access may bypass these guarantees.

---

## 17. Publication failure and recovery

The publication phase is not a true distributed ACID transaction.

Example:

```text
workspace lock acquired

X/main: x0 -> xA      succeeds
Y/main: y0 -> yA      succeeds
workspace commit      fails/crashes
```

The workspace state remains W0, therefore readers still see a consistent state by following the exact member snapshots recorded in W0.

However member canonical branches are now ahead of the published workspace.

A retry in the same process can detect that the member branch pointers already equal the transaction targets and retry only the workspace commit.

For crash recovery, the publication protocol should persist a small transaction record before modifying member canonical branches.

Conceptually:

```text
transaction_id: TA
base_workspace: W0
state: PREPARED

targets:
    X: x0 -> xA
    Y: y0 -> yA
```

The publication sequence becomes:

```text
1. acquire lock
2. validate
3. persist PREPARED transaction record
4. fast-forward member branches
5. commit workspace snapshot
6. clear/mark transaction record complete
7. release lock
```

A future recovery routine can determine whether an interrupted transaction should be completed or rolled back.

The initial implementation may keep recovery minimal, but transaction metadata should be recorded from the start so that later recovery does not require a format redesign.

---

## 18. Garbage collection

Garbage collection becomes a workspace-level responsibility because member IceChunk repositories do not know which of their snapshots are referenced from workspace snapshots.

Eventually `ZFWorkspace.gc()` should consider:

```text
retained workspace branches/tags
    ↓
reachable workspace snapshots
    ↓
referenced member repository snapshots
    ↓
referenced managed-file manifests
    ↓
referenced immutable files
```

It may then clean:

- abandoned transaction branches;
- unpublished member snapshots;
- old member snapshots not reachable from any retained workspace state;
- orphaned managed files older than a safety grace period.

Destructive independent GC/expiration of member repositories should not be used unless coordinated with the workspace.

GC is deferred from the minimal working product.

---

## 19. Minimal working product

The first implementation should focus on the core semantics and avoid advanced repository-history features.

### Required

- `ZFWorkspace` with one workspace IceChunk repository;
- registration/resolution of multiple member `ZFRepository` instances;
- one member repository per bucket/prefix as the normal permission boundary;
- persistent workspace snapshot mapping `{repository_id -> snapshot_id}`;
- persistent application state in the workspace repository;
- `WorkspaceRoot`, `RepositoryRoot`, `RepositoryChild`, `FilePrefix` node distinction;
- read-only opening strictly side-effect free;
- transaction-private member repository branches;
- root-only repository commits;
- workspace-level coordinated commit;
- short workspace publication lock;
- conditional fast-forward integration into member canonical branches;
- conservative workspace conflict detection;
- read/write-set tracking at repository granularity;
- explicit workspace tags for permanent application states;
- immutable external files through `fsspec`;
- per-prefix Zarr/IceChunk file manifest;
- compact content-derived physical filenames;
- UTF-8 string support for manifests;
- verification of managed file content;
- safe logical-path normalization and checkout.

### Deferred

- `repo.schemas()` history/indexing;
- `node.revisions()` DAG traversal;
- automatic workspace rebase;
- repository-level true merge of divergent transaction branches;
- sophisticated crash recovery UI;
- workspace-aware garbage collection;
- automatic cleanup of abandoned transaction branches;
- file-manifest sharding for extremely large flat prefixes;
- use of IceChunk `flush()` in workspace coordination.

---

## 20. Core invariants

The design should preserve the following invariants:

1. **One `ZFRepository` corresponds to one IceChunk repository.**
2. **One `RepositoryRoot` owns exactly one IceChunk session.**
3. **All descendants of a `RepositoryRoot` share that session.**
4. **Only the `RepositoryRoot` may commit the repository session through the high-level API.**
5. **`WorkspaceRoot` has no public IceChunk session; its commit is a coordinator operation.**
6. **A workspace snapshot is the authoritative state of the whole application.**
7. **Member repository branch heads alone never define workspace state.**
8. **Workspace-managed writes use transaction-private member branches.**
9. **Member canonical branches are modified only during the short serialized workspace publication phase.**
10. **Repository integration is conditional fast-forward only in the MVP.**
11. **Readers follow exact member snapshot IDs stored in the workspace snapshot and therefore never observe partially published workspace transactions.**
12. **Operational storage configuration is excluded from schema identity.**
13. **Managed external files are immutable and identified by content.**
14. **The exact original logical filename/path is preserved in the manifest.**
15. **File uploads and member commits may leave orphans on failure; correctness is preserved and later GC removes them.**

---

## 21. Main open questions

The following details remain to be resolved during implementation:

- exact persistent representation of workspace member-repository mapping;
- exact format and storage location of the workspace application state;
- repository ID allocation and storage-location resolution;
- exact workspace publication lock implementation across supported backends;
- transaction journal representation and minimum crash-recovery behavior;
- exact namespace for transaction-private branches;
- exact canonical member branch naming (`main` or workspace-specific canonical branches);
- exact read-set tracking semantics;
- precise handling of explicit member `RepositoryRoot.commit()` before `WorkspaceRoot.commit()`;
- exact schema canonicalization and hash algorithm;
- exact manifest Zarr schema and variable-length UTF-8 encoding;
- compact content-ID encoding length/alphabet;
- physical filename truncation rules;
- handling of logical rename/move of managed files;
- policy for member repositories that are inaccessible to a particular user but referenced by an otherwise readable workspace snapshot.

These do not change the main architecture and can be resolved incrementally.
