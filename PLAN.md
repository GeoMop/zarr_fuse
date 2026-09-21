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
  

  
  
## Planned Work: Merged Overlay Tile Build + Upload Script

### Goal

Merge the overlay preprocessing chain (GCP VRT, warp, RGBA expansion,
gdal2tiles) and the S3 tile upload into one new standalone script that runs
the whole flow automatically. No zf_view.yaml or config.py changes in this
task; the script is CLI-driven with Bukov defaults.

### Approved decisions (2026-08-24)

- Single new file `dashboard/scripts/build_overlay_tiles.py`; old
  `prepare_bukov_gcps.py` and `test/upload_s3.py` stay until the merged
  script is proven.
- Parameters via CLI args with current Bukov defaults (zooms 0-20,
  EPSG:4326/3857, near resampling, xyz scheme).
- Upload credentials from `dashboard/scripts/.env` (`ZF_S3_ACCESS_KEY`,
  `ZF_S3_SECRET_KEY`) plus `--endpoint-url` / `ZF_S3_ENDPOINT_URL`;
  validated only when upload will run.
- Basic skip logic: steps skipped when their output exists unless
  `--force`; upload skips keys whose S3 object size matches local file.
- `--dry-run` prints the planned commands without executing (also skips
  GDAL tool checks so it works on machines without GDAL).

### Pipeline

1. gdal_translate -of VRT -a_srs <gcp_srs> -gcp ... image -> vrt
2. gdalwarp -t_srs <target_srs> -r <resampling> -dstalpha -> tif
3. gdal_translate -of VRT -expand rgba tif -> rgba vrt
4. gdal2tiles --xyz -z <min>-<max> rgba vrt -> tiles/
5. upload changed tiles/<z/x/y>.png -> s3://<bucket>/<prefix><relpath>

### Verification

- py_compile, argparse --help.
- --dry-run against fixture assets in temp dir (no GDAL needed).
- Negative test: real run without GDAL tools exits with clean error.
- Full pipeline run needs the conda GDAL env and real assets; handed to
  the user as a manual command.

## Planned Work: Config Wiring For build_overlay_tiles.py

### Goal

Wire the merged tile script parameters to zf_view.yaml (the `tile_build`
section) and credentials to the general environment / `.env`; no behavior
change for dry-run, skip logic, or upload payload handling.

### Approved decisions (2026-08-24)

- Import `dashboard.config` after sys.path bootstrap; on missing deps exit
  with a `pip install pyyaml python-dotenv` hint.
- View selection: `--view` flag > `HV_DASHBOARD_VIEW` env >
  `_dashboard.default_endpoint`.
- Views file: `--view-path` flag > `ZF_VIEW_PATH`/upward search > repo
  default path.
- Parameter precedence: CLI flag > config value; generic processing defaults
  (zoom range, CRS values, resampling) are owned by the config layer
  (`TileBuildConfig`), and the S3 bucket/prefix target must be configured
  explicitly (`tile_build.s3`) or passed per-run via `--bucket`/`--prefix`.
  The startup summary prints where each value came from.
- S3 endpoint URL stays schema-owned (`ATTRS.S3_ENDPOINT_URL`);
  `--endpoint-url` remains as manual override.
- Upload gate: `--skip-upload` flag > `tile_build.enabled` >
  `tile_build.upload_enabled`; credentials validated early only when an
  upload run will actually happen (dry-run and local builds stay
  credential-free).
- Relative config paths resolve against `base_dir`
  (views_path.parent.parent = `app/databuk`).
- `cache_dir` formalization deferred to a later step.

### Implementation

- `app/databuk/config/zf_view.yaml`: added `upload_enabled` plus nested
  `s3: {bucket, prefix}` under `bukov_endpoint.tile_build`; no secrets in
  YAML.
- `dashboard/config.py`: new `TileS3Config(bucket, prefix)` dataclass;
  `TileBuildConfig.upload_enabled` and `.s3` fields; parsed in
  `_build_view_config()` via new `_build_tile_s3_config()` helper.
- `dashboard/scripts/build_overlay_tiles.py`: full rewire to the view config
  via `find_view_file` / `get_default_endpoint_name` /
  `load_environment_from_config` / `load_view_config` / `schema_endpoint_url`;
  removed `--view-dir` and script-local `.env` loading (env file now comes
  from `_dashboard.env_file`).

### Verification

- Sandbox dry-run through a temporary `ZF_VIEW_PATH`: every parameter
  resolved from config with source labels, schema endpoint picked up,
  graceful message when the configured env_file is absent, upload count
  computed from fixture tiles.
- Negatives: fail-fast missing-credentials error when upload is enabled;
  credential-free dry-run; `upload_enabled: false` variant skips upload
  without requiring credentials; missing GDAL tool exits with clean error.
- CLI flag overrides show `(flag)` sources in the startup summary.
- Suite baseline with `s3_tile_resolver_test.py` ignored: 115 passed,
  1 skipped.
- Full real-GDAL pipeline run requires the conda GDAL env and real assets;
  handed to the user as a manual command.

## Planned Work: Ensure-Tiles-On-S3 Model

### Goal (2026-08-24, user decision)

Tile creation and upload are one nested operation ("either both work or
none"): check whether the tile system already exists on S3; if it does,
do nothing; if not, build locally and upload in the same run.

### Approved semantics

- Script is standalone/operator-triggered only (local terminal); not a
  workflow stage, so no coupling to dashboard runtime state. The
  `overlay_enabled()` gate was removed in the review pass below.
- Existence criterion: any object under `s3://bucket/prefix`
  (`list_objects_v2` MaxKeys=1). Non-empty prefix = tile system present =
  run ends immediately, GDAL never touched.
- No staleness detection. `--force` is the manual rebuild lever: bypasses
  the S3 short-circuit AND local output skips (full redo); size-compare
  still avoids re-transferring byte-identical tiles.
- Local per-step output skips stay (without `--force`) so an interrupted
  run can resume.
- Credentials and S3 target are required for a real run; `--dry-run` stays
  fully offline.
- Removed: `tile_build.enabled`, `tile_build.upload_enabled`,
  `--skip-upload`.
- 2026-08-24 review pass (script destined to leave the dashboard project as
  a standalone local tool): removed the `overlay_enabled()` gate - an
  operator running the script has already decided they want tiles, and the
  dashboard gates its own tile serving independently (tile_service,
  serve_dashboard routes, map_views). Also removed the parsed-but-unused
  `tile_scheme` and `add_alpha` fields (script hardcodes `--xyz` and
  `-dstalpha`) and fixed a stray `[1/5]` label in the GCP VRT skip path
  (now `[2/6]`). Old scripts `prepare_bukov_gcps.py` and
  `test/upload_s3.py` stay until the script is actually moved out.

### Implementation

- `app/databuk/config/zf_view.yaml`: `tile_build` reduced to a pure work
  order (paths, params, s3 target); gates removed.
- `dashboard/config.py`: `TileBuildConfig` lost `enabled`/`upload_enabled`
  fields and their parsing.
- `dashboard/scripts/build_overlay_tiles.py`: flow is now gate ->
  existence check ([1/6]) -> local build ([2/6]-[5/6]) -> upload ([6/6]);
  shared `_s3_client()` factory used by check and upload;
  `tiles_exist_on_s3()` wraps `ClientError` with bucket/endpoint context.

### Verification

- Sandbox matrix: gate off exits silently; dry-run prints offline check +
  full plan; `--dry-run --force` reports the bypass; real run without creds
  fails fast; missing s3 target errors naming the yaml keys; parse of the
  slimmed yaml OK.
- Suite baseline with resolver ignored: 115 passed, 1 skipped.
- Real end-to-end (conda GDAL env + real credentials) remains a manual step.

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

## Planned Work: Variable-Switch Consistency + Dashboard Plot Cleanup

### Goal

Fix the reported dashboard bug: switching the left-panel variable (e.g.
`rock_temp` -> `air_temp`) leaves the "Plot selection" dropdown on the old
variable and can plot old-variable data under new-variable titles/labels.
Simplify the leftover complexity from the previous "replace stale data in
place" approach.

### Cause

Two distinct defects triggered by the same action:

- Defect A (dropdown): `plot_var_selector.options` is set once to a single
  item (`composed.py:180`) and never updated; only `value` is changed on
  variable switch (`composed.py:207`), which a dropdown cannot display when
  the value is not in `options`.
- Defect B (labels vs. data): titles/Y-axis read `data.display_variable`
  immediately (`multi_time_views.py:25-33,403`), while plotted series live in
  `selection_state` and only refresh via a best-effort re-fetch loop
  (`composed.py:372-384`) that can silently skip (out-of-range index) or error,
  leaving stale data.

### Design

Single root variable `data.display_variable` drives every indicator; no new
or inherited indicator variable is introduced. On variable change, use a
centralised save -> clear -> refetch -> restore flow so labels and data
always agree. Remove the now-unneeded `add_site(force=True)` replace path and
the generic re-fetch loop from `refresh_views`.

### Steps

1. `composed.py`: rewrite `_select_variable` to save entity indices + exact
   checked `(site_id, depth)` set, `selection_state.clear()`, refetch saved
   sites (stable entity index resolved from shared lat/lon coords), restore
   the checked subset, report per-site failures in the service status, and
   sync `plot_var_selector.options` with `value`.
2. `composed.py`: simplify `refresh_views` — drop the `saved_indices` capture
   and the `lats`/`lons` re-fetch loop. Safe because the other callers
   (`_switch_view`, `on_node_change`) already `clear()` first.
3. `plot_selection.py`: remove the `force` parameter and in-place replace
   branch from `add_site`; keep a single plain add that auto-checks finite
   depths.
4. `multi_time_views.py`: call plain `add_site`; surface fetch errors so
   callers can report per-site failures.
5. Update and extend tests in `dashboard/test/`.

## Planned Work: Helm Chart Standardization + CI Secrets (2026-09-21)

### Goal

Review of `dashboard/charts/holoviz` and the dashboard workflows (user request, 2026-09-21). Findings:

- Resource names (`holoviz-frontend`, `holoviz-frontend-secrets`, `holoviz-ingress`) ignore the
  release name, so two releases cannot coexist in one namespace; the Service selector matches pods
  of any release.
- Only `app.kubernetes.io/name` is set on resources; no `helm.sh/chart`, `instance`, `managed-by`
  outside the pod template, no `_helpers.tpl`, chart name (`holoviz-dashboard`) differs from its
  directory (`holoviz`).
- CI creates the S3 Secret through `helm --set` from GitHub secrets (values and secrets end up in
  the release history) and a ConfigMap through `kubectl` outside the release (unmanaged, fixed
  name `dashboard-config`, collides between releases).
- `dashboard-pull-request.yaml` and `dashboard-push-main.yaml` filter on `app/databuk/dashboard/**`,
  a path that does not exist, so they never trigger; the PR one duplicates
  `holoviz-dashboard-pull-request.yaml` (same namespace and release) and would race it if fixed.

### Steps

1. Chart: add `templates/_helpers.tpl` (`holoviz.name/fullname/chart/labels/selectorLabels`,
   host helper), rename chart to `holoviz`, bump to 0.3.0, `nameOverride`/`fullnameOverride`.
   All resources named from `fullname`, standard labels everywhere, selector = name + instance.
2. Chart: `frontend.s3.existingSecret` (reference a pre-created Secret with `ZF_S3_*` keys); the
   templated Secret is rendered only when it is empty. Empty credential defaults +
   `values/minimal-required-values.yaml` for lint (same pattern as the ingress-server chart).
3. Chart: `frontend.config.*` renders a release-scoped ConfigMap from files staged under
   `charts/holoviz/config/` (`.Files.Glob`) and mounts it; sets `ZF_VIEW_PATH`/`SCHEMAS_PATH`.
   `ingress.enabled/host/tls` values with the current e-infra defaults.
4. Reusable workflow: stage `zf_view.yaml` + schemas into the chart instead of `kubectl create
   configmap`; new `s3-secret-name` input with a preflight `kubectl get secret` check and a
   printed create command; the `--set` credential path stays as a deprecated fallback for external
   callers; `configmap_name`/`mount-volume-name` inputs kept but ignored (callers would break on
   unknown inputs). Lint with the minimal values file.
5. Caller workflows: use `s3-secret-name`, fix path filters, add `concurrency` per release,
   remove the duplicate `dashboard-pull-request.yaml`.
6. Chart README: prerequisites (Secret created once per namespace), values, multiple releases.

### Verification

- `helm lint --strict` with the minimal values, `helm template` for two release names in one
  namespace (distinct resource names, selectors carry `instance`), template with staged config.
- Workflow YAML parses; no cluster access from this environment, so no live deploy test.

## AGENT Questions And Remarks

- The reported variable-switch bug is actually two independent defects with a
  shared trigger: Defect A (stale `plot_var_selector.options`) is a display
  widget sync issue; Defect B (labels update before/without the refetched
  data) is a data/label decoupling via `selection_state`. Both were fixed
  with the clear-then-refetch design; the root-variable simplification
  removes the in-place replace machinery (`add_site(force=True)`, the
  `refresh_views` re-fetch loop).
- The variable-switch "only default site moves" symptom was traced to
  `_refetch_sites` re-fetching via `map_state` positional indexing + `on_map_tap`
  nearest-marker resolution. Refetch is now by stored `entity_index` directly
  via a new `fetch_site_entity` wrapper. Open follow-up (not in scope): the
  live map-click path feeds `tap_stream.x/y` (likely Web-Mercator meters) into
  `on_map_tap`, whose nearest math treats them as degrees - a separate,
  pre-existing site-selection unit/CRS concern worth checking later. User
  confirmed entities load to the map whenever coordinates exist (values are
  not filtered by `get_map_data`).
- The earlier "save -> clear -> refetch -> restore" design in "Planned Work"
  was superseded by an in-place refresh (2026-08-31): `_select_variable` no
  longer `clear()`s - `SelectionState.update_site_data` swaps each site's
  series/times in place (bumping `version` only, so the plot-selection table
  and map do not rerender on a pure variable change). The map's overlay tiles
  and point positions are variable-independent, so `refresh_views` reuses
  `map_state` on variable change instead of rebuilding the map. Sites for
  which the new variable has no data are blanked (empty series/depths) and
  reported in the status. Bug-3 "has_value" value-coloring on the map remains
  out of scope/deferred.
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

- 2026-08-24 (overlay automation): the overlay source assets are not in the
  repo anymore (`dashboard/config/bukov_endpoint/` deleted). Resolved
  2026-08-24: the build script no longer uses a `--view-dir` default; all
  paths now come from the selected view in zf_view.yaml, resolved relative
  to `app/databuk`. Remaining open question: where should overlay
  inputs/outputs live in the long term, and should generated tiles stay
  gitignored build artifacts?

- 2026-08-24: Running `pytest dashboard/test` from the repo root fails at
  collection: pytest selects `dashboard/pyproject.toml` as inifile (closer
  to the args than the root `pytest.ini`), whose default `python_files`
  matches `*_test.py`, so the module-level code in
  `s3_tile_resolver_test.py` executes on import and raises
  (`HV_DASHBOARD_VIEW is required`). Unrelated to the new build script;
  rewriting that resolver to be import-safe is already planned as a later
  step of the overlay automation work. Until then use
  `--ignore=dashboard/test/s3_tile_resolver_test.py`.

- 2026-09-21 (chart / CI review, open decisions):
  - USER: the S3 Secret is now expected to exist as `zarr-fuse-s3` in `zarr-fuse-dashboard` and
    `zarr-fuse-dashboard-development` (keys `ZF_S3_ACCESS_KEY`, `ZF_S3_SECRET_KEY`,
    `ZF_S3_ENDPOINT_URL`). Create it before the next deploy (command in the chart README and in the
    workflow's preflight error); the `S3_ACCESS_KEY` / `S3_SECRET_KEY` GitHub secrets can then be
    removed. Different name wanted?
  - `dashboard-reusable-workflow.yaml` now has the layout of `ingress-server-reusable-workflow.yaml`.
    External callers (HLAVO) must move from `source_repo` / `source_ref` / `source-views-path` /
    `source-schemas-path` / `frontend_image` / `configmap_name` / `mount-volume-name` and the
    lowercase secrets to `views-path` / `schemas-path` / `docker-repository` / `s3-secret-name` with
    `secrets: inherit` (`KUBECONFIG`, `DOCKER_PASSWORD`); the caller repository is checked out
    directly, so no token inputs remain.
  - `ingress_server/charts/ingress-server` has the same shape (fixed names via `app.name`, only
    `app.kubernetes.io/name`, credentials via `--set`); apply the same `_helpers.tpl` +
    `existingSecret` treatment there?
  - The PR workflow still overwrites the single `holoviz-development` release. With release-scoped
    names a per-PR release (`holoviz-pr-<number>`, uninstalled on PR close) is now possible.
  - The reusable workflow still creates the namespace from CI when the kubeconfig allows it; keep,
    or require pre-created namespaces like the ingress-server workflow does?
  - `dashboard-push-main.yaml` deploys the `dashboard-main` branch, not `main`; intended?

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
- 2026-08-13: Made the dashboard tile endpoint URL strict schema-only. Added
  `dashboard/tile_service.py::_endpoint_url_from_schema()` which resolves
  `ATTRS.S3_ENDPOINT_URL` from the endpoint schema file (endpoints.yaml ->
  `source.schema_path`, resolved against `endpoints_path.parent.parent`);
  removed the `ZF_S3_ENDPOINT_URL` env read and the now-dead
  `S3_ENDPOINT_URL` mapping line in `dashboard/serve_dashboard.py`; dropped
  `ZF_S3_ENDPOINT_URL` from `dashboard/scripts/.env`. Dashboard suite: 116
  passed.
- 2026-08-13: DRY cleanup of `_endpoint_url_from_schema()`: replaced the
  hand-rolled endpoints.yaml/schema-path walking with the existing config
  resolvers (`get_default_endpoint_name` + `get_endpoint_config` from
  `dashboard/config.py`), which also restores `SCHEMAS_PATH` support that the
  first version silently dropped. Dashboard suite: 116 passed.
- 2026-08-13: Moved the endpoint URL resolution into `dashboard/config.py` as
  `resolve_endpoint_url(config_path, endpoint_name=None)` (reads
  `ATTRS.S3_ENDPOINT_URL` from the endpoint schema, defaulting to the
  configured default endpoint). `dashboard/tile_service.py` now calls it with
  its own endpoints-path resolution and optional `HV_DASHBOARD_ENDPOINT`.
  Dashboard suite: 116 passed.
- 2026-08-13: DRY consolidation of the tile service path resolution: removed
  the duplicated `_resolve_endpoints_path()` from `dashboard/tile_service.py`
  and reused `config.resolve_endpoints_path()` instead. The module now resolves
  `ENDPOINTS_PATH` once at import, wrapped in try/except `FileNotFoundError`
  to keep the fail-soft (never crash) behavior. Callers of
  `resolve_endpoint_url` / `_cache_dir_from_endpoints` guard on
  `ENDPOINTS_PATH is not None`. Dashboard suite: 116 passed.
- 2026-08-13: Config flow simplification (S2 + S1 renames): `get_endpoint_config`
  replaced by default-aware `load_endpoint_config(config_path, endpoint_name=None)`;
  `resolve_endpoints_path` renamed to `find_endpoints_file`, `resolve_endpoint_url`
  renamed to `schema_endpoint_url`. All callers updated (tile_service, serve_dashboard,
  composed, data, excluded s3_tile_resolver_test). Added brief lead-in docstrings to the
  config flow functions. Dashboard suite: 116 passed.
- 2026-08-13: Full dashboard-config sweep "endpoint" -> "view" (approved by user;
  concrete instance key `bukov_endpoint` kept). Renamed `config.py` API to
  `ViewConfig`, `load_views`, `load_view_config`, `get_default_view_name`,
  `find_view_file`, `_build_view_config`, `_collect_group_fields(view_name)`; env
  vars `ZF_VIEW_PATH` + `HV_DASHBOARD_VIEW` with deprecated legacy fallbacks
  (`ENDPOINTS_PATH`, `HV_DASHBOARD_ENDPOINT`, yaml `default_endpoint`).
  `schema_endpoint_url()` kept (S3 concept), param/internals renamed to view.
  `data.py` -> `ViewHandle`, `LocalClient(views_path)`, `get_views`/`get_view`,
  `DashboardData.view_name`. All UI modules, `tile_service.py` (`VIEWS_PATH`,
  `_cache_dir_from_view`), scripts (`check_view_stores.py` replaces
  `check_endpoint_stores.py`), chart (`frontend.viewName`, `HV_DASHBOARD_VIEW`),
  and workflows (`source-views-path`, `zf_view.yaml` configmap) updated.
  Config file `app/databuk/config/zf_view.yaml` (old endpoints.yaml deleted).
  Docs swept. Dashboard suite: 116 passed.
- 2026-08-14: Added YAML single-parse caching for zf_view.yaml. New
  `_parse_view_config(path)` in `config.py` with process-lifetime dict cache;
  replaced all direct `yaml.safe_load(f)` calls for the view config
  (`load_environment_from_config`, `load_views`, `get_default_view_name`,
  `tile_service._cache_dir_from_view`). Also removed redundant
  `load_environment_from_config` call inside `load_views`. Dashboard suite:
  115 passed, 1 skipped (test_dataset_variable_report — S3 secret-gated, now
  properly skips).
- 2026-08-14: Overlay gating (zero tile cost when disabled). New
  `overlay_enabled(config_path, view_name)` in `config.py` centralizes env var
  check (`HV_OVERLAY_ENABLED`) + config check. `tile_service.py` gated at
  import: boto3 client, CACHE_DIR, ENDPOINT_URL all `None` when disabled.
  `serve_dashboard.py` ROUTES conditional on `OVERLAY_ENABLED`.
  `map_views._load_overlay` refactored to use shared `overlay_enabled` helper
  (was missing env var check before). Dashboard suite: 115 passed, 1 skipped.
- 2026-08-14: Fixed overlay gating regression: `overlay_enabled()` and
  `tile_service.py` now resolve `_VIEW_NAME` from `_dashboard.default_view`
  when no env var (`HV_DASHBOARD_VIEW`/`HV_DASHBOARD_ENDPOINT`) is set.
  `overlay_enabled(VIEWS_PATH, None)` was returning False because
  `config.get(None)` → None. Routes were never registered, causing 404s on
  all `/tiles/` requests. Dashboard suite: 115 passed, 1 skipped.
- 2026-08-14: Reverted YAML key `default_view` back to `default_endpoint` in
  `zf_view.yaml`. `get_default_view_name()` now reads `default_endpoint` first,
  `default_view` as compat fallback. User decision: the YAML key stays as
  `default_endpoint`. Dashboard suite: 115 passed, 1 skipped.
- 2026-08-14: Renamed `get_default_view_name` → `get_default_endpoint_name`,
  removed `default_view` compat fallback. YAML key stays `default_endpoint`.
  Dashboard suite: 115 passed, 1 skipped.
- 2026-08-24: Planned and implemented the merged overlay tile build + upload
  script `dashboard/scripts/build_overlay_tiles.py` (CLI-driven, Bukov
  defaults, skip logic, dry-run); config files intentionally untouched.
- 2026-08-24: Verified the new script: py_compile OK, --help OK, --dry-run
  against fixture assets prints the full command plan (disabled GCP filtered,
  existing-output skip works, upload count computed) without GDAL installed;
  negative paths exit cleanly (missing GDAL tool, missing georef file, fewer
  than 3 enabled control points, missing S3 credentials). Dashboard suite
  with `s3_tile_resolver_test.py` ignored: 115 passed, 1 skipped (= baseline).
- 2026-08-24: Wired build_overlay_tiles.py to project configuration. Added
  `TileS3Config` plus `upload_enabled`/`s3` parsing to `dashboard/config.py`,
  extended `bukov_endpoint.tile_build` in zf_view.yaml (`upload_enabled`,
  nested `s3.bucket`/`s3.prefix`), and rewrote the script to resolve the
  view, paths, and build params from zf_view.yaml via dashboard.config
  helpers (flag > config > default precedence, per-value source summary).
  Endpoint stays schema-owned; credentials come from the general env filled
  by `_dashboard.env_file`. Fixed views-file precedence so `ZF_VIEW_PATH`
  beats the repo default, and restored early credential validation for real
  upload runs. Dashboard suite with `s3_tile_resolver_test.py` ignored:
  115 passed, 1 skipped (= baseline). Real-GDAL end-to-end run pending user
  execution in the conda gdal environment.
- 2026-08-24 (review pass, ensure-model): removed the `overlay_enabled()`
  gate from build_overlay_tiles.py - it is a standalone local tool, not a
  workflow stage, so it no longer reads the dashboard display toggle.
  Removed parsed-but-unused `tile_scheme`/`add_alpha` from
  `TileBuildConfig` (config.py) and their yaml lines; fixed the GCP VRT
  skip label `[1/5]` -> `[2/6]`. Old scripts
  `prepare_bukov_gcps.py` / `test/upload_s3.py` kept until the script is
  moved out of the repo. Dashboard suite with `s3_tile_resolver_test.py`
  ignored: 127 passed, 1 skipped (baseline grew from 115 by 12 tests via
  external commit e32cfa2 adding test_validation_phase2.py, unrelated).
- 2026-08-24: Default cleanup in the tile pipeline (user decision "move
  generics to config.py"): removed all script-side `DEFAULT_*` constants
  from build_overlay_tiles.py except `DEFAULT_VIEWS_PATH` (bootstrap only).
  Generic processing defaults (zoom 0-20, EPSG:4326/3857, near resampling)
  moved into `TileBuildConfig` field defaults and `_build_view_config()`
  parsing. `tile_build.s3.bucket`/`prefix` are now required for real upload
  runs (fail-fast error naming the yaml keys; `--skip-upload`/disabled views
  and dry-run stay requirement-free, dry-run prints an `(unset)`
  placeholder). Removed the silent Bukov test-bucket fallback. Dashboard
  suite with `s3_tile_resolver_test.py` ignored: 115 passed, 1 skipped
  (= baseline); sandbox scenarios verified (normal dry-run sources,
  missing-s3 error path, skip-upload path, unset placeholder).
- 2026-08-24: Removed the hardcoded `DEFAULT_VIEWS_PATH` from
  build_overlay_tiles.py (user decision: use the shared finder as in
  composed.py). `_resolve_views_path()` is now flag > `find_view_file()`
  (`ZF_VIEW_PATH` env, then upward cwd search), with a clean error hint when
  nothing resolves. Consequence: running the script by absolute path from
  outside the repo now requires `ZF_VIEW_PATH`, `--view-path`, or a cwd
  inside the repository - same semantics as other dashboard entry points.
  Fixed latent dry-run crash found during verification:
  `importlib.util.find_spec("osgeo.utils")` raises `ModuleNotFoundError`
  for the missing parent package `osgeo`; new `_has_module()` helper treats
  that as "not installed". Dashboard suite with `s3_tile_resolver_test.py`
  ignored: 115 passed, 1 skipped (= baseline). Resolution paths verified:
  repo-cwd search, env override, flag precedence, foreign-cwd error and
  foreign-cwd-with-env success.
- 2026-08-24: Switched the tile pipeline to ensure-tiles-on-S3 semantics
  (user decision after Q&A). Single gate is now `overlay.enabled` /
  `HV_OVERLAY_ENABLED` via `config.overlay_enabled()`; removed
  `tile_build.enabled`, `tile_build.upload_enabled`, and `--skip-upload`.
  New stage [1/6] checks for any object under `s3://bucket/prefix`
  (`tiles_exist_on_s3()`) and short-circuits when present; build ([2/6]
  -[5/6]) and upload ([6/6]) run atomically when missing; `--force` = full
  redo bypassing short-circuit and local skips; no staleness detection;
  dry-run stays offline. Shared `_s3_client()` factory now serves both check
  and upload. zf_view.yaml `tile_build` reduced to paths/params/s3 target;
  `TileBuildConfig` lost its gate fields. Dashboard suite with
  `s3_tile_resolver_test.py` ignored: 115 passed, 1 skipped (= baseline).
  Real end-to-end run pending user execution in the conda GDAL environment.
- 2026-08-31: Implemented the variable-switch consistency fix + dashboard
  plot cleanup (see "Planned Work: Variable-Switch Consistency + Dashboard
  Plot Cleanup"). Key items: `_select_variable` now saves -> clears ->
  refetches (stable entity index) -> restores the checked depth subset and
  syncs `plot_var_selector.options` with `value`; `refresh_views` no longer
  runs the `saved_indices`/`lats`/`lons` re-fetch loop; `add_site` lost its
  `force` replace path; `_fetch_timeseries` surfaces errors for per-site
  status reporting.
- 2026-08-31: Fixed the variable-switch "only default site moves" bug. The
  root cause: `_refetch_sites` re-fetched each site via `map_state` positional
  indexing + `on_map_tap` nearest-marker/threshold resolution, which is fragile
  (coordinate filtering / CRS units) and could drop or mis-target non-default
  sites. Fix: `build_timeseries_views` now returns a `fetch_site_entity`
  wrapper that re-fetches deterministically by the stored `entity_index`
  (via `get_timeseries_data`'s native entity_index path); `_refetch_sites` loops
  saved entity indices directly and reports per-site failures. Wired
  `map_handlers["fetch_site"]` in `build_dashboard` + `refresh_views`. Added
  `dashboard/test/test_variable_switch.py` (3 tests). Suite: 121 passed,
  1 skipped (S3 secret-gated).
- 2026-08-31: Replaced the variable-switch clear->re-add flow with an in-place
  refresh so the plot-selection table and map view no longer rerender on a pure
  variable change (timeseries plots only redraw). Added
  `SelectionState.update_site_data` + `_reconcile_checked` (bump `version` only
  when depths unchanged; also `layout_version` + prune checks on depth change).
  `_fetch_timeseries` now updates an existing site in place and blanks a site
  (empty series/depths) when the new variable has no data. `composed.py`
  `_select_variable`/`_run_refresh` drops the save/`clear()`/`restore_checked`
  snapshot; `_refetch_sites()` iterates live sites; `refresh_views(rebuild_map=)`
  skips `build_map_view` on variable change (reuses `map_state`). Rewrote
  `test_variable_switch.py`   to the in-place model and added
  `test_update_site_data.py`. Suite: 126 passed, 1 skipped (S3 secret-gated).
- 2026-08-31: Fixed variable-switch plots not updating when the selected
  variable's data is all NaN (title changed, no error toast, but curve stayed
  on old data). Root cause: `_run_refresh` rebuilt the plot closures before
  refetching, so freshly-built DynamicMaps first rendered pre-refetch (old)
  state and relied on the `version` stream to re-render; combined with a stale
  `default_display_variable` capture in `build_timeseries_views`. Fix:
  `_fetch_timeseries` now reads the live `data.display_variable` at call time
  (removed the `default_display_variable` capture and `metric_label` derives
  from `data.display_variable` directly), and `_run_refresh` now refetches
  sites in place before `refresh_views(rebuild_map=False)` so the rebuilt plots
  render already-updated data. Added all-NaN regression test. Suite: 127
  passed, 1 skipped (S3 secret-gated).
- 2026-09-01: Generalized the store-health scan script. Renamed
  `dashboard/scripts/scan_coordinate_health.py` to
  `dashboard/scripts/scan_store_health.py` and removed all bukov / lat-lon
  specifics. The script now behaves like `df.info()` for any zarr-fuse store:
  it takes a schema YAML (via the `--schema` CLI arg or the `SCHEMA_PATH`
  top-of-file constant, plus the `GROUP_PATH` constant) and reports coordinates
  + data variables (dtype, dims, shape, size, missing count, and per-coordinate
  value samples under `--verbose` / `--only-missing`). The bukov-specific
  lat/lon-per-borehole CSV diagnostic (`--latlon`), the hardcoded location-CSV
  paths, the `STORE_URL` constant, and the hardcoded `children["bukov"]` group
  navigation were removed. Credentials stay env-driven (`ZF_S3_*` / `S3_*`).
Verified locally: `py_compile`, `--help`, blank/not-found schema error paths
   (exit 2), and end-to-end scans against a synthetic local store at root and
   with `--group` navigation.
- 2026-09-09 (overlay builder, user test): Completed a fresh-user test of
  `build_overlay_tiles.py` end to end: installed Miniconda, created `gdal-test`
  conda env, installed GDAL (conda-forge) + `pyyaml`/`python-dotenv`/`boto3` via
  pip, and processed `test_overlay/source.png` (360x360) into 7326 XYZ tiles
  uploaded to `s3://app-databuk-test-service/test_tiles/_test/`. Re-run
  confirms the idempotent short-circuit ("already present"). The test found 3
  setup bugs, all fixed:
  (1) `setup_gdal_env.ps1` relied on `$ErrorActionPreference="Stop"` which does
  NOT catch nonzero exits from native commands (`conda`/`pip`), so it printed
  false "created"/"installed" after real failures; rewrote with `$LASTEXITCODE`
  checks aborting at the failing step (and `-PassThru` exit-code check after the
  Miniconda installer).
  (2) Anaconda 2026 enforces Terms of Service for its `defaults` channels,
  blocking non-interactive `conda create`/`install`; both setup scripts now
  reconfigure conda to conda-forge-only channels
  (`config --remove channels defaults`, `--add channels conda-forge`, `--set
  channel_priority strict`) before creating the env, so the ToS wall is gone
  and GDAL is only ever pulled from conda-forge.
  (3) The missing-GDAL hint referenced `dashboard/scripts/setup_gdal_env.ps1`
  relative to the repo root, failing when run from inside `dashboard/scripts`;
  it now prints absolute paths derived from `Path(__file__)` for both setup
  scripts, plus the absolute path of the build script itself.
  Verified: `py_compile`, PowerShell parse of the rewritten ps1, dry-run
  unchanged, missing-GDAL message correct from the repo root and the scripts
  folder, dashboard suite 129 passed / 1 skipped (`s3_tile_resolver_test.py`
  ignored).
  OPEN / REMARKS: (a) the probe reported "Miniconda not found" even though
  `C:\Users\fatih\miniconda3` existed earlier, and installed a fresh Miniconda
  there - the exact `Scripts\conda.exe` existence check needs re-confirmation
  on a machine with a pre-existing conda. (b) The user now has two conda
  installs (`miniconda3` freshly installed, `miniconda3-new` original with the
  old `gdal-test` env); a cleanup/consolidation decision is pending. (c) The
  rewritten `setup_gdal_env.sh` could not be executed on this Windows-only
  machine (no bash); only syntax-reviewed.
- 2026-09-09 (overlay builder, follow-up): Per user request, the builder now
  removes locally generated intermediates automatically after a successful S3
  upload (`--no-cleanup` keeps them). Added `_cleanup_local_tiles()`; source
  inputs (image, georef points) are never deleted, only the generated VRTs, the
  warped GeoTIFF, and the local tile tree.
- 2026-09-09 (overlay builder, channel fix finished): Verifying the rewritten
  setup script exposed that the effective conda channel pool still contained
  `defaults`: conda merges the user `.condarc` with a conda-root `.condarc`
  (`<condaRoot>\.condarc`, here written by the earlier manual ToS-accept
  experiment), and the root file kept `[defaults]`. Step [2/6] now patches
  BOTH files (remove `defaults`, add `conda-forge`, `channel_priority strict`);
  verified `conda config --show channels` now reports only `- conda-forge`.
  Also fixed two Windows PowerShell 5.1 traps found while rerunning the script:
  (a) redirecting a native command's stderr (`2>$null`) under
  `$ErrorActionPreference="Stop"` turns benign messages into terminating
  `NativeCommandError`s; the tolerant `--remove` call now toggles the
  preference around itself, and no other native call redirects stderr -
  exit-code checking via `Assert-Command` is the real safety net. (b) Running
  the whole script under a harness `2>&1` reproduces the same terminating
  behavior even for unredirected child stderr; a plain terminal run is
  unaffected (the interrupted run output was a harness artifact).
  Verified: script completes all 6 steps on the real install, steps
  idempotent, effective channels conda-forge only, dashboard suite 129 passed /
  1 skipped, `py_compile` OK, line lengths <=120.
  OPEN / REMARKS: the truly fresh-install path (no existing conda at all) has
  still not been exercised by the final script; the user machine already had a
  conda with ToS accepted, so the TOS-wall claim relies on the conda-forge-only
  channel pool and has not been proven on a virgin machine.
- 2026-09-10 (overlay builder, S3 test-tile cleanup): Added a ``--delete`` mode
  to ``build_overlay_tiles.py``: deletes every object under the resolved
  ``tile_build.s3`` prefix instead of building/uploading, reusing the same
  config/credential/endpoint resolution. ``--delete`` requires ``--yes``
  (prints the resolved target and aborts otherwise); ``--delete --dry-run`` is
  read-only and reports the object count; an empty effective prefix is refused
  (no bucket-root deletion); ``--force``/``--no-cleanup`` are invalid with
  ``--delete`` and ``--yes`` without ``--delete`` is an error. Deletion runs
  ``delete_objects`` in batches of 1000 and re-lists to verify 0 objects
  remain. Executed the real cleanup of the test pyramid: 7326 objects under
  ``test_tiles/_test/`` in ``app-databuk-test-service`` were deleted and
  independently verified (count 0). The next build run will rebuild/re-upload
  because the S3 short-circuit is now free. Verified: py_compile, --help,
  flag-combination errors, dry-run preview (7326), real delete (7326/7326),
  suite 129 passed / 1 skipped, line lengths <=120. README tile section not
  updated (optional follow-up).
  OPEN / REMARKS: `test_overlay/` and the tile intermediates in
  `app/databuk/test_overlay/` were kept for the cleanup feature demo; both were
  uploaded successfully, so the workspace can be cleaned at git-cola review.
- 2026-09-10 (overlay builder): Zoom-cap decision - test view limited to
  `max_zoom: 10` (user: "this is only for the test"); production default
  `max_zoom: 20` in `dashboard/config.py` unchanged, per-run raises possible
  via `--max-zoom`. Note: coverage data showed the dashboard tile cache only
  requested zoom 14-15, so an overlay capped at z10 renders only while zoomed
  out (expected, documented).
- 2026-09-10 (overlay builder, missing output dir): User's first clean-slate
  run failed at step [2/6] with `ERROR 4: Failed to open
  .../app/databuk/test_overlay/source_gcps.vrt to write` - gdal_translate/
  gdalwarp/gdal2tiles do not create parent directories, and the new auto-
  cleanup had removed `app/databuk/test_overlay/`. Fixed in
  `build_overlay_tiles.py`: after tile_build path validation,
  `if not args.dry_run: for out in (vrt_path, tif_path, rgba_vrt_path,
  tiles_dir): out.parent.mkdir(parents=True, exist_ok=True)`. Only parents are
  created (never `tiles_dir` itself) so the [5/6] skip-if-exists logic is
  preserved and gdal2tiles still makes the tile dir. Dry-run stays pure:
  verified it creates nothing.
- 2026-09-10 (overlay builder, sources under config): Moved the test sources
  `source.png` + `georef.json` from (top-level) `test_overlay/` into a new
  `app/databuk/config/source_overlay/` per user request ("gather these sources
  under this directory"), and updated `zf_view.yaml`:
  `source_image: "config/source_overlay/source.png"`,
  `georef_file: "config/source_overlay/georef.json"` (resolved relative to
  app/databuk base dir). Intermediates stay under `app/databuk/test_overlay/`.
  Verified end-to-end in the `gdal-test` env: build + upload + auto-clean in
  7.3 s; only **10 tiles** at z0-10 (7326 before the zoom cap); re-run
  short-circuits ("Tile system already present"); S3 count verified = 10;
  dry-run prints the new source paths and creates nothing; suite 129 passed /
  1 skipped; py_compile OK; line lengths <=120.
  OPEN / REMARKS: `app/databuk/config/test.png` (6185 B, a stale duplicate of
  `source.png`) is still untracked and unused by the config - candidate for
  removal at git-cola review. Empty root-level `test_overlay/` dir left in
  place (untracked, invisible to git).
- 2026-09-10 (overlay serving pixel-aligned with builder): Added
  `visualization.overlay.source_uri` to `zf_view.yaml`
  (`s3://app-databuk-test-service/test_tiles/_test/`, single URI form, user
  choice) and wired it into the dashboard tile service so it serves tiles from
  exactly the location `build_overlay_tiles.py` writes, fixing a prefix
  mismatch (S3TileHandler used `PREFIX = TILE_PREFIX or "test_tiles/"` while
  the builder uploads to `test_tiles/_test/`). Precedence (user choice: full):
  `source_uri` wins outright when present; `TILE_BUCKET`/`TILE_PREFIX` env and
  the `test_tiles/` default remain only as fallback. Changes:
  `dashboard/config.py` `OverlayConfig.source_uri` + parser; new
  `_overlay_source_from_view()` in `dashboard/tile_service.py` (parses `s3://
  bucket/prefix` via urlparse, reuses `_parse_view_config` like
  `_cache_dir_from_view`), applied at module init when OVERLAY_ENABLED.
  Verified: resolution overrides bogus TILE_BUCKET/TILE_PREFIX env; presigned
  URL for request `(10,552,346)` targets `test_tiles/_test/10/552/346.png`
  which exists in S3 (HEAD 954 B); config parse exposes `source_uri`;
  py_compile OK; suite 129 passed / 1 skipped; lines <=120.
  REMARKS: gdal2tiles skipped z0-1 (no tiles for levels the image does not
  fill), so the pyramid starts at z2; missing z0/z1 requests just 404 like any
  absent XYZ tile. The overlay still renders only up to z10 and the dashboard
  cache previously requested z11-15 - raising max_zoom is still an open user
  decision if the overlay must show at normal dashboard zoom.
- 2026-09-10 (overlay quality): Resampling was only applied to the gdalwarp
  step; `step_generate_tiles` passed no `-r`, so gdal2tiles used its hardcoded
  `near` on every pyramid level. Quality plan (user approved) implemented:
  (1) `step_generate_tiles` now takes the resolved `resampling` and passes
  `-r <resampling>` to gdal2tiles; main() threads it through. (2) Test config
  `app/databuk/config/zf_view.yaml`: `resampling: "cubic"` and `max_zoom: 15`
  (source 360x360 over ~1.4x2.2 km -> native ~1:1 around z14-15 at lat
  50.08, covering the dashboard's z14-15 request range). (3) Production default
  `max_zoom: 20` in `dashboard/config.py` is intentionally unchanged for non-
  test uploads. Verification limited to tests by user request (no S3/real run):
  py_compile OK, dashboard suite 129 passed / 1 skipped, lines <=120.
  NEXT (user takes over): from dashboard/scripts in gdal-test env run
  `python build_overlay_tiles.py --delete --yes` then `python
  build_overlay_tiles.py` to rebuild/upload the z2-15 cubic pyramid, then check
  the overlay at z14-15 in the dashboard.
- 2026-09-10 (real overlay): Switched the test view from the synthetic
  360x360 source to the real delivery: `12p_final.png` (7085x13938, 300 DPI,
  P-mode scan, ~99 Mpx) georeferenced with 3 QGIS control points
  (`bukov_georef.json`, WGS84/EPSG:4326; duplicate copies `bukov_georef2.json`
  GeoJSON + `12p_final.png.points` carry the same points). Footprint across
  the 3 GCPs: ~16.220-16.229E / 49.458-49.481N (Brno region, matching the real
  bukov site data - the earlier Prague placement was just the test source).
  `build_gcp_args` already flips the QGIS negative-sourceY convention via
  `abs()` (pixel_y -> 11806.9/8412.7/299.1, all inside the 13938 rows), so no
  script change was needed; only config. Deployed edits in `zf_view.yaml`:
  `source_image`/`georef_file` -> the real files, `max_zoom: 20` (user's
  non-test rule; native detail is ~z19 so z20 oversamples 2x),
  `resampling: "cubic"` (unchanged), S3 `prefix: "overlays/bukov/"` (same test
  bucket/endpoint), and `overlay.source_uri` updated to match. Expected volume
  flagged to user: ~5-7k tiles at full res. Georef residuals are large
  (341-760 px ~ 75-170 m) so some alignment error is possible - flagged.
  Verified (tests only, no S3/build per user boundary): config parse prints
  real source/20/cubic/overlays-bukov, loader produces 3 in-range GCPs,
  py_compile OK, suite 129 passed / 1 skipped, lines <=120. Real build/upload
  left to the user: `python build_overlay_tiles.py` in dashboard/scripts.
- 2026-09-18 (timeseries markers per window, done): Borehole markers now have
  an X-step relative to the visible window (5 markers per window X range) and
  reset live on pan/zoom. Implemented in `dashboard/multi_time_views.py`:
  (1) `build_timeseries_overlay` stores curves only (dropped the baked
  `curve * scatter` and its full-range `n_markers=8` positions), (2) new pure
  helper `_resolve_marker_window(x_range, xlim, times)` prefers the live
  `x_range` when valid, else view `xlim`, clamped to data bounds, (3) new
  `build_marker_overlay(view, x_range, xlim)` builds 5 scatters per combo via
  `np.interp`, (4) compose per render `curves * markers * vline` in
  `create_timeseries_view` so the existing RangeX streams recompute markers on
  every start/end change. Added `dashboard/test/test_marker_window.py` (8
  tests: live range, None/NaT fallback to xlim, malformed range, clamping to
  bounds, inverted range, empty times, partial out-of-data clamp). Suite:
  137 passed / 1 skipped (baseline 129 + 8 new).
- 2026-09-21 (markers stale RangeX on center change, done): After moving the red
  line, mid/right initial views showed no borehole markers until the next pan/zoom.
  Cause: `RangeX` only updates on pan/zoom, so on a center change
  `_resolve_marker_window` preferred the stale `x_range` over the freshly
  computed `xlim`, placing markers outside the visible window. Fixed in
  `dashboard/multi_time_views.py` `create_timeseries_view` by guarding the marker
  window source with per-view `_marker_state` (last (version, center) key +
  last adopted x_range): re-anchor to `xlim` on every key change (red-line move,
  variable switch, initial load) and only adopt live `x_range` when it differs
  from the stored baseline (a real pan/zoom). `build_marker_overlay` and
  `_resolve_marker_window` unchanged; marker unit tests unaffected.
- 2026-09-10 (real overlay, line-art readability): User reports numbers/small
  notes on the zoomed-in overlay lose pixels ("missing pixels"). Cause: single
  `resampling` knob fed `cubic` into both gdalwarp and gdal2tiles; cubic (and
  lanczos) smear thin strokes over semi-transparent background - right for
  photos, wrong for scanned line art. Implemented separate resampling knobs
  (Option 2, user approved): new `TileBuildConfig.tile_resampling`
  (`dashboard/config.py`, parsed from `tile_build.tile_resampling`);
  `build_overlay_tiles.py` resolves `tile_resampling = tile_build.
  tile_resampling or resampling` (no CLI flag), passes it to
  `step_generate_tiles`, header prints both values. `zf_view.yaml`: warp
  `resampling: "near"` (preserves every stroke through the geometric warp) +
  `tile_resampling: "average"` (downsampling merges ink density). Kept
  `max_zoom: 20` - E-W native detail is ~z19.6-z20 (7085 px / 0.63 km ->
  0.089 m/px) while N-S is z19 (13938 px / 2.6 km -> 0.187 m/px), so dropping
  to 19 would halve horizontal detail; z20 with near now carries real pixels
  (cubic previously made the extra zoom meaningless). Verified tests only:
  py_compile, config parse (near/average), suite 129/1, lines <=120.
  NEXT: user rebuilds (`python build_overlay_tiles.py`), A/B zoom on numbers;
  fallback ladder: tiles->near; if the source lacks pixels at 100% add a
  stroke-contrast/alpha-threshold post-step.
- 2026-09-21 (Helm chart standardization + CI secrets, done): `dashboard/charts/holoviz` renamed to
  `holoviz` (0.3.0) with `templates/_helpers.tpl` (`name`, `fullname`, `chart`, `labels`,
  `selectorLabels`, `host`, `tlsSecretName`, `s3SecretName`, `configMapName`); Deployment, Service,
  Ingress, Secret and the new ConfigMap are named from `fullname`, carry the standard
  `app.kubernetes.io/*` + `helm.sh/chart` labels and select on name + instance + component.
  `frontend.s3.existingSecret` skips the templated Secret; `frontend.config.enabled` renders a
  ConfigMap from files staged under `charts/holoviz/config/` (git-ignored) with a checksum
  annotation, replacing the `kubectl create configmap` step and the `extraEnv` / `extraVolumes`
  wiring; `ingress.enabled/host/tls`, `nameOverride` / `fullnameOverride`,
  `values/minimal-required-values.yaml` for lint. Workflows: reusable workflow rewritten
  in the style of `ingress-server-reusable-workflow.yaml` (kebab-case inputs with defaults, uppercase
  secrets, `prepare-configuration` job uploads `zf_view.yaml` + schemas as an artifact that the
  deploy job downloads into `charts/holoviz/config/`, `alpine/k8s` deploy container, plain
  `helm upgrade` with `frontend.s3.existingSecret` from `s3-secret-name`, no kubectl secret or
  configmap handling); callers use `secrets: inherit` and job-level `concurrency`, path filters fixed
  (`app/databuk/dashboard/**` did not exist); duplicate `dashboard-pull-request.yaml` removed. Chart README rewritten.
  Verified: `helm lint --strict` (minimal values) clean, `helm template` for two releases in one
  namespace gives distinct names, ConfigMap content round-trips byte-identical, `required` /
  no-staged-files errors fire, ingress toggles work; workflow YAML parses, caller inputs and secrets
  cross-checked against the reusable workflow, actionlint clean apart from two pre-existing
  shellcheck infos. No cluster access here, so no live deploy.
