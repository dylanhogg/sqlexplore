.EXPORT_ALL_VARIABLES:
DEV=True

venv:
	# Install https://github.com/astral-sh/uv on macOS and Linux:
	# $ curl -LsSf https://astral.sh/uv/install.sh | sh
	# Other recommended libraries, add with `uv add <library>`:
	# tenacity, joblib, jupyterlab, litellm, datasets, pytorch, fastapi, uvicorn, rich
	uv sync
	uv pip install -e .

which-python:
	uv run which python | pbcopy
	uv run which python

clean:
	rm -rf .venv

run:
	uv run python -m sqlexplore.app data/example.parquet

run-http:
	uv run python -m sqlexplore.app https://github.com/dylanhogg/awesome-python/raw/refs/heads/main/github_data.parquet

run-as-tool:
	uv run sqlexplore data/example.parquet

run-as-docker:
	docker compose run --rm app gnaf.parquet

docker-build:
	docker compose build --no-cache

test:
	uv run pytest -vv --capture=no tests

test-as-docker:
	docker compose run --rm tests

manual-checks:
	uv run ruff format .
	uv run ruff check . --fix
	uv run pyright

precommit-install:
	# One time: Install git hook to run pre-commit automatically on git commit
	# Uninstall with: uv run pre-commit uninstall
	uv run pre-commit install

precommit:
	uv run pre-commit run --all-files

build-dist:
	uv build

publish-check:
	uv publish --dry-run

publish-pypi:
	@test -n "$$UV_PUBLISH_TOKEN" || (echo "UV_PUBLISH_TOKEN is required"; exit 1)
	uv publish

.DEFAULT_GOAL := help
.PHONY: help build-dist publish-check publish-testpypi publish-pypi
help:
	@LC_ALL=C $(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print $$1}}' | sort | egrep -v -e '^[^[:alnum:]]' -e '^$@$$'
