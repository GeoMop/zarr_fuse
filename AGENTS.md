# ZARR-FUSE project agents rules

## Project Summary

Zarr-fuse is serious production project asking for top level code quality, 
and backward compatibility once the version 1.0.0 is reached. Meanwhile
we are still in the early phase and we can break the API with new 0.x.0 versions.
Marking an API feature deprecated first.


## Structure
- `app` - some test applications of the zarr-fuse project, DEPRECATED going to move these into separate projects.
- `dashboard` - HoloViz platform based dashboard for data overview
- `ingress_server` - a custom service for both passive and active data collection
- `zarr_fuse` - main library sources
- `zarr_fuse/tests` - tests of the main library 
- `tools` - various scripts, notably snipets for S3 bucket operations
- `tests` - top level and prerequisity / depencencies tests
- `zf` - stub of a cli for cmd line store operations


## CODEX Ignore Folders

- `**/venv`
- `**/.tox`
- `**/.pytest_cache`
- `**/build`
- `**/dist`
- `**/*.egg-info`

## Workflow

These workflow rules apply to this repository, including `AGENTS.md`,
`PLAN.md`, and `python_coding.md`.

- The user reviews changes in `git-cola`. Do not commit changes unless
  explicitly asked.
- Before editing, check the repository state and avoid overwriting unrelated
  user changes.
- At the beginning of work, check the request against `AGENTS.md`, `PLAN.md`,
  `README.md`, and relevant docs/tests.
- If `PLAN.md` does not exist yet, review `_PLAN.md` if present and create a
  fresh `PLAN.md` before larger work.
- Do not ask for confirmation before making requested changes unless the
  required intent cannot be inferred from the repository context.
- Keep each change focused on one function, module concern, or coherent
  refactoring.
- Do not mix planning edits with code implementation unless explicitly
  requested.
- For larger edits, update `PLAN.md` with the intended steps and unresolved
  questions before implementation.
- Put unresolved project questions or inconsistencies in the last section of
  `PLAN.md` under `AGENT Questions And Remarks`.
- Use the `AGENT log` section in `PLAN.md` for concise completed-work records.
- Treat `AGENT` notes in source comments or documentation as direct
  instructions. When resolved, add a short `Resolved:` line after the note and
  let the user remove the note later.
- For documentation-only changes, tests are not required.
- For code changes, run targeted tests first, then broader verification when
  the change affects shared behavior.
- When tests depend on secrets, remote services, or network access, separate
  them clearly from local deterministic tests and prefer explicit skip/mark
  behavior over implicit failures in developer environments.
- Do not add real secrets to tracked files. Use environment variables or
  placeholders in examples and test fixtures.

## Coding Rules

Include and adapt: `python_coding.md`.

Project specific rules:
- All MD files have hard limit 120 chars per line.
- Iteratively add type hints and doc strings to touched code. 
- Do not change anything you do not understand well.
- Rather ask before change.

  
## Mandatory Finish Checklist

Before the final response, verify these items explicitly:

- `PLAN.md` has been reviewed for relevant current work.
- Any touched `AGENT` notes have following `Resolved:` lines.
- New unresolved questions or inconsistencies are recorded in `PLAN.md`.
- The final response mentions open `USER:` questions, missed requirements, and
  failed or skipped verification.
# zarr-fuse project guidelines
