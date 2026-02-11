# PyPI Release Hardening Plan (`sqlexplore`)

## Goals

- Make publish flow deterministic (`uv build`, `uv publish`).
- Ensure install/run works on target Python versions and common machine setups.
- Tighten package metadata + dependencies for long-term maintainability.

## Current Risks (from audit)

1. Python support too narrow for many users: `requires-python = ">=3.13"`.
2. `uv publish --dry-run` panics in current local toolchain (`uv 0.9.24`).
3. No explicit `[build-system]` in `pyproject.toml`.
4. Runtime dependency metadata not fully aligned with imports (direct `rich` import, unused deps likely present).
5. Remote download path is cwd-relative (`data/downloads`), can fail on non-writable working dirs.
6. PyPI-facing metadata/readme can be improved (classifiers, URLs, install docs).

## Plan

### 1. Packaging Metadata Hardening

1. Add explicit `[build-system]` in `pyproject.toml`:
   - `requires = ["setuptools>=70", "wheel"]`
   - `build-backend = "setuptools.build_meta"`
2. Add/verify project metadata:
   - `project.urls` (`Homepage`, `Repository`, `Issues`)
   - `classifiers` (Python versions, license, OS, topic)
   - `keywords`
   - author/maintainer fields (if desired)
3. Keep `readme = "README.md"` and `license` metadata explicit and valid.

### 2. Python Version Strategy

1. Decide support target:
   - Option A: Keep `>=3.13` (simpler, smaller support surface).
   - Option B: Lower to `>=3.11` (broader adoption).
2. Align tooling to chosen floor:
   - Ruff target version.
   - Pyright pythonVersion.
3. Add explicit statement of supported Python versions in README.

### 3. Dependency Tightening

1. Reconcile runtime imports vs declared dependencies:
   - Ensure direct imports are direct deps (`rich` currently used directly).
2. Remove runtime deps not used by shipped app paths (if any).
3. Keep dev-only tooling in `[dependency-groups].dev`.

### 4. Runtime Robustness Across Machines

1. Replace hardcoded relative download location with configurable option:
   - Add `--download-dir`.
   - Default to user-writable cache/data location.
2. Improve error messaging for unwritable paths.
3. Add tests for non-default download directory behavior.

### 5. README + PyPI UX Cleanup

1. Add install section:
   - `pip install sqlexplore`
   - `uv tool install sqlexplore` (optional).
2. Add quick smoke examples:
   - local file
   - remote URL
   - `--version`
3. Add supported Python versions and known limitations.
4. Remove maintainer-only/manage URLs from user-facing README text.

### 6. Publish Pipeline Reliability

1. Fix local publish tooling issue:
   - Upgrade/reinstall `uv`.
   - Re-run `uv publish --dry-run` until stable.
2. Add repeatable pre-release command sequence:
   - `uv run ruff format .`
   - `uv run ruff check . --fix`
   - `uv run pyright`
   - `uv run pytest -q`
   - `uv build`
   - smoke install from wheel
   - smoke install from sdist
   - `uv publish --dry-run`
3. Only publish when all checks pass.

### 7. Release Execution

1. Bump version in `pyproject.toml` to next unpublished version.
2. Build and verify artifacts in `dist/`.
3. Publish:
   - `uv publish` (with `UV_PUBLISH_TOKEN`).
4. Post-publish validation:
   - fresh venv install from PyPI
   - run `sqlexplore --version`
   - run one non-interactive query (`--no-ui`)

## Definition of Done

1. `uv build` succeeds with explicit build backend config.
2. `uv publish --dry-run` succeeds without panic.
3. Wheel + sdist install and run in fresh environments for supported Python versions.
4. Dependencies and metadata accurately reflect runtime behavior.
5. README is PyPI-user oriented and complete for first-run success.
