# PLAN

## Status Snapshot

- `PLAN.md` is the active planning document for current repository work.
- `_PLAN.md` contains older branch-specific review notes and is treated as
  historical input, not the active plan.
- Current user priority: fix a small set of failing tests, first separating
  true code failures from failures caused by missing secrets or remote access.

## Active Priorities

### P0. Triage failing tests

- Identify the currently failing tests and group them into:
  - deterministic local failures
  - S3 or secret-gated failures
  - hangs or timeout regressions
- Reproduce local failures with targeted pytest invocations before any broader
  suite run.
- For S3-backed tests, prefer explicit skip conditions when required secrets or
  endpoints are unavailable.
- Add a shared pytest fixture to load local secret environment variables from
  `.secrets_env` for tests that opt into remote-backed execution.

### P1. Stabilize local store open behavior

- Continue the investigation from `_PLAN.md` around local store open hangs in
  `zarr_fuse/zarr_storage.py`.
- Prioritize `test_open_store[options0]`, `test_node_tree[local]`, and related
  local-only coverage before touching S3 behavior.
- Add or refine a deterministic regression test once the blocking path is
  confirmed.
- 2026-06-20 status: local and S3 store-open tests now pass in the current
  environment, so this item is no longer the active blocker.

### P1. Review empty dataset first-write semantics

- Revisit the `__empty__` handling and first-write path in
  `zarr_fuse/zarr_storage.py`.
- Ensure first update does not destructively reset sibling metadata or child
  groups.
- Add regression coverage for empty-to-first-write transitions.

### P2. Tighten schema and read-path consistency

- Reconcile the `composed` attribute contract across write and read code paths.
- Fix or confirm the coordinate uniqueness validation noted in `_PLAN.md`.
- Add tests only after the intended behavior is explicit.

### P2. Fix interpolation lookup for unsorted dims

- Reproduce the `KeyError` in `interpolate.py` when an unsorted dimension is
  present in `ds_sorted` but absent from `interp_coords`.
- Fix the nearest-coordinate selection to iterate over `interp_coords.items()`
  and read the size from `ds_sorted` for the same key.
- Add a regression test that covers a dataset with a sorted dimension and an
  unsorted singleton dimension.

### P2. Separate S3 compatibility work

- Keep S3 async/sync behavior review separate from local test stabilization.
- Avoid mixing environment setup issues with library regressions in one change.
- When S3 tests are expected to require secrets, document that requirement in
  the test or fixture entry point.

### P2. Warning triage

- Classify current pytest warnings before deciding whether to suppress, fix, or
  document them.
- Separate warnings into:
  - external/library compatibility warnings
  - test-design warnings
  - likely project bugs or unsafe conversions
  - stale warnings already removed by newer edits

#### Warning classification snapshot

- `zarr_fuse/test/test_zarr_storage.py::test_open_store[options1]`:
  `PytestUnraisableExceptionWarning` from `tkinter.Variable.__del__` and
  `tkinter.Image.__del__` with `RuntimeError: main thread is not in main loop`.
  Classification: external side effect / test-environment cleanup issue.
  Notes: likely caused by GUI objects being created indirectly and finalized
  during pytest teardown outside a Tk main loop; not obviously related to the
  storage logic under test.
  
  AGENT: what is the origin of the error? where Tk is used in the test, no Tk use is expected there.
  2026-06-20 investigation:
  - does not reproduce in isolated `test_open_store[options1]`
  - `zarr_fuse.__init__` imports optional `zarr_fuse.plot`, which imports
    `matplotlib`, but backend probing currently reports `agg`, not Tk
  - current evidence points to cross-test session state or another test/module
    importing a Tk-backed plotting path elsewhere, not the storage open path
    itself
  - next step should be warning-order narrowing across the broader suite rather
    than changing `test_open_store` or storage code blindly
  2026-06-20 update:
  - narrowed further to test-side plotting imports in
    `zarr_fuse/test/test_tools.py` and `zarr_fuse/test/test_interpolate.py`
  - those tests now force the non-interactive `Agg` backend and close figures
    explicitly, which removes the Tk destructor warnings in mixed local runs
  - a separate issue remains in `test_open_store[options1]`: S3 cleanup in
    `_wipe_store()` can block inside `fsspec.asyn.sync(...)`, but that is not
    the Tk warning itself
  
  AGENT: we need an explicit distinction if we want the test plots. Postpone this rignt now.

- `zarr_fuse/test/test_zarr_storage.py::test_merge_ds_unsorted`:
  `UnstableSpecificationWarning` for `FixedLengthUTF32` under Zarr v3.
  Classification: external/library format warning with schema/design impact.
  Notes: points to string dtype persistence using a Zarr v3 representation that
  is not yet stable across implementations.
  
  AGENT: Postponed until we resolve UTF support in a separate issue.


- `zarr_fuse/test/test_zarr_storage.py::test_node_tree[local]`:
  `ZarrUserWarning` from `zarr/core/group.py` that object at `logs` is not
  recognized as part of a Zarr hierarchy.
  Classification: project layout / metadata-structure warning.
  Notes: likely caused by zarr-fuse storing logging artifacts adjacent to Zarr
  groups in a way current zarr traversal notices and warns about.
  
  AGENT: Where it comes from? Does current code stores more then a string messages? 
  A serious issue as it possibly breaks the logging mechanism.
  2026-06-20 investigation:
  - origin is consistent with `StoreLogHandler` in `logger.py`, which writes
    plain log payloads under `logs/YYYYMMDD.log` inside the same root store
  - Zarr hierarchy traversal warns because `logs/` is not a Zarr-native group
    or array component, so this is a namespace/layout issue rather than a
    string-format issue
  - current implementation stores plain UTF-8 formatted log lines only; it does
    not store structured metadata beyond those text payloads
  - likely remedies to evaluate later:
    1. move logs outside the Zarr store namespace
    2. reserve and ignore the `logs/` prefix during hierarchy traversal
    3. encode logs in a Zarr-native array/group structure

  AGENT: Postponed, we are going to introduce metadata zarr store to capture
  preovenence and log data specific to particular dataset updates.
    
  
*active warning issues*
  

  
  
## Working Rules For This Plan

- Prefer small, focused fixes over wide refactors while test failures are still
  being triaged.
- Update this file before larger code changes and after major findings.
- Record unresolved repo questions in the last section of this file.

## Planned Work: Delayed date_time Merge

### Goal


4. Add unsupported schema-key diagnostics.
   - Add validation for unsupported keys in schema config dictionaries.
   - Prefer a clear error for unsupported nested `merge` blocks and other
     ignored keys, so old or misspelled schema items cannot silently change
     merge behavior.

6. Update tests after implementation.
   - For default `merge = None`, assert sorted `date_time`, assert delayed
     in-range values are not inserted, and assert an error is logged.
   - For stepped merge, assert intermediate coordinates are introduced after the
     19th update.
   - For stepped merge, assert generated intermediate dependent values are `NaN`
     before the real delayed update arrives.
   - For stepped merge, assert delayed real 18th and 17th night updates write
     into the generated grid instead of appending unsorted values.
   - For both modes, keep the final sorted-coordinate assertion.

## AGENT Questions And Remarks

- Some existing tests already skip when S3 credentials are absent, but the repo
  still mixes local and remote assumptions. That boundary should be made more
  explicit during test-fix work.
  
- Current pytest triage shows two separate failure classes:
  - S3-backed tests reach the remote endpoint but fail with `AccessDenied` on
    `test-zarr-storage`, so the injected credentials are present but do not
    have the required bucket permissions for these tests.
  - Local weather/time tests fail in timezone conversion paths and likely hinge
    on `DateTimeUnit.tz_shift` using the current date's offset for `CET`.

- After bucket access was fixed, only the weather/time failures remained.
  Current assessment:
  - `TestPivotND::test_pivot_nd_weather` exposes a real conversion bug for
    `CET` source timestamps due to DST-sensitive offset lookup.
  - `test_update_weather` expectations appear inconsistent with the schema
    contract (`source_unit: CET`, `unit: UTC`) and likely need alignment to the
    corrected UTC conversion behavior.

- Repo-local pytest secret loading also needed env-name bridging:
  some code paths consume `ZF_S3_*`, while others still read `S3_*`.

- Warning triage should keep apart:
  - warnings that document deliberate Zarr v3 compatibility limits
  - warnings that expose project behavior we may want to make explicit
  - warnings already eliminated by recent test changes but still present in
    older warning logs

- Real Bukov worker reproduction under `ingress_server/tests/` is currently
  blocked in this environment before merge/update: opening the S3-backed store
  for `rancher-bukov-moc-test.zarr` fails with
  `botocore.exceptions.ParamValidationError` for an empty object key during
  root-group access. That appears distinct from the original sorted-coordinate
  assertion and needs dependency/path handling review before the data bug can
  be reproduced end-to-end.

- 2026-06-30 local verification of datetime storage tests is currently blocked
  before the delayed-update assertion: both the new regression and existing
  `test_datetime_encoding_roundtrip` hang in `zarr.open_group` during the first
  local `zf.open_store()` call. This conflicts with the older note that local
  store-open behavior was no longer active and needs fresh triage.

- AGENT: Should the sorted `any_new` schema warning be emitted only for the
  implicit default, or also for an explicit `step_limits: "any_new"` or
  equivalent schema spelling?
  
## AGENT log

- 2026-06-20: Reviewed `AGENTS.md`, `README.md`, `python_coding.md`, and
  `_PLAN.md`.
- 2026-06-20: Created a structured `PLAN.md` aligned with the current user
  priority of failing-test triage.
- 2026-06-20: Updated `AGENTS.md` to clarify plan ownership and secret-gated
  test handling.
- 2026-06-20: Confirmed repo-local `venv` and `.secrets_env` for pytest-based
  failure triage.
- 2026-06-20: Added a session pytest fixture to load repo-local secret env
  files and ran `venv/bin/pytest` for baseline failure classification.
- 2026-06-20: Re-ran the previously failing storage/weather subset after S3
  bucket access was fixed; remaining work is isolated to datetime handling.
- 2026-06-20: Fixed timezone abbreviation handling for `CET` by using a fixed
  offset instead of the current date's DST-sensitive offset.
- 2026-07-02: Reworked datetime step-limit conversion to use unit polymorphism
  instead of `Coord.step_limits_delta_array()`.
- 2026-07-02: Removed `delta_unit_instance()` and made `Interval.step_limits()`
  use `cfg.get(..., default_unit)` with coordinate unit methods.
- 2026-07-03: Introduced `DeltaUnit` for step-unit context and fixed simplified
  `delta_array()` handling for missing and fractional step units.
- 2026-06-20: Aligned weather test expectations with the schema contract for
  explicit UTC input timestamps.
- 2026-06-20: Extended pytest secret loading to support both repo-root and
  `tools/` secret files and to publish `S3_*` aliases from `ZF_S3_*`.
- 2026-06-20: Removed implicit autouse secret-env loading from
  `zarr_fuse/test/conftest.py`; tests now opt in via the secret fixture path.
- 2026-06-20: Simplified `test_prototype_repro.py` to use fixed S3 endpoint
  and bucket constants, removed duplicate local `.env` handling, and replaced
  the async direct-S3 write path with the same minimal fsspec/zarr pattern
  used by the passing compatibility test.
- 2026-06-20: Reworked
  `test_write_with_pure_zarr_read_with_zarr_fuse` to use a minimal inline
  schema plus pure-zarr arrays with explicit `dimension_names`, which avoids
  the previous invalid-endpoint and event-loop failures and now passes along
  with the full `test_prototype_repro.py` module.
- 2026-06-20: Reviewed the current warning list and classified each warning in
  `PLAN.md` as external compatibility noise, test-environment cleanup,
  likely project bug, or stale warning pending re-verification.
- 2026-06-20: Fixed two high-severity warning sources: forbidden complex->real
  lossy casts now raise explicitly, and degenerate linear interpolation now
  falls back to nearest/P0 interpolation without the SciPy divide warning.
- 2026-06-20: Removed test-driven consolidated-metadata warnings by keeping the
  affected tests on the unconsolidated metadata path.
- 2026-06-20: Narrowed warning-origin analysis for `logs/` hierarchy traversal
  and the non-reproducible Tk destructor warning.
- 2026-06-20: Made plotting tests explicitly headless (`Agg`) and close their
  figures, removing the Tk teardown warnings from mixed local pytest runs.
- 2026-06-20: Confirmed the remaining `logs/` warning is caused by plain text
  log files written inside the Zarr namespace, not by tree reconstruction.
- 2026-06-22: Started interpolation regression work for unsorted singleton
  dimensions in `interpolate.py`.
- 2026-06-22: Fixed `interpolate_ds` nearest-coordinate lookup for unsorted
  singleton dimensions and added a regression test in
  `zarr_fuse/test/test_interpolate.py`.
- 2026-06-25: Reproduced the ingress sorted-coordinate assertion with a
  synthetic `interpolate_ds` datetime-coordinate test and improved the
  diagnostic to report the first offending adjacent coordinate pair.
- 2026-06-25: Updated the Bukov test extractor fixture to accept the current
  worker payload contract and load borehole metadata from test fixtures; real
  `_process_one` reproduction now advances to S3 store open and is blocked
  there by an empty-key `ParamValidationError`.
- 2026-06-30: Added local `Node.update` regression tests for delayed
  `date_time` batches written through separate reopened nodes. Both the
  `date_time.merge = None` and `date_time.merge.step_limits = [15, 61, "m"]`
  cases currently assert only that stored `date_time` coordinates are sorted
  and print the stored coordinate size for debugging.
- 2026-07-01: Completed delayed `date_time` plan steps 1-3: confirmed renamed
  tests from commit `a950233`, unified sorted-coordinate diagnostics in
  `interpolate.py`, migrated known schema fixtures to direct `step_limits`,
  fixed datetime step-unit handling in `zarr_schema` so `"m"` means minutes,
  and confirmed the stepped update path creates intermediate coordinates with
  `NaN` dependent values before delayed data arrives.
- 2026-07-01: Confirmed with the user that `merge = None` maps to
  `step_limits = no_new`, affects only coordinate extension, and keeps the
  default overwrite policy for existing coordinates.
- 2026-07-01: Confirmed with the user that `step_limits` is a direct
  coordinate-level schema item; nested `merge: {step_limits: ...}` is wrong
  and test/schema fixtures should be migrated.
- 2026-07-01: Confirmed with the user that `step_limits: [15, 61, "m"]`
  provides bounds for the step-selection algorithm, which preserves input
  times rather than forcing a fixed 15-minute or hourly grid.
- 2026-07-07: Reverted the incorrect step 5 interpretation that allowed
  `no_new` tail extension and updated the step 5 plan to treat `no_new` as no
  new coordinate values at all.
- 2026-07-07: Implemented delayed `date_time` plan step 5 for sorted
  `step_limits: None` / `no_new`, including rejected-coordinate logging,
  no-op handling for fully rejected updates, and focused regression coverage.
- 2026-07-07: Confirmed with the user that `no_new` allows no new coordinate
  values at all, while existing coordinates near the incoming coordinate range
  should still be updated through the current interpolation behavior.
- 2026-07-07: Resolved the `no_new` no-overlap case by logging the rejected
  coordinate update and returning the existing dataset instead of raising the
  previous `No data was written to the dataset` assertion.
- 2026-07-08: Simplified `interpolate_ds()` to reindex with all coordinates
  returned from `interpolate_coord()` and tightened `merge_ds()` so empty
  multidimensional extension subsets remain no-op writes.
