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

run-example-local:
	uv run python -m sqlexplore.app data/example.parquet

run-example-http:
	uv run python -m sqlexplore.app https://github.com/dylanhogg/awesome-python/raw/refs/heads/main/github_data.parquet

run-example-images-1:
	uv run sqlexplore https://huggingface.co/datasets/mteb/tiny-imagenet/resolve/main/data/valid-00000-of-00001-70d52db3c749a935.parquet

run-example-images-2:
	uv run sqlexplore https://huggingface.co/datasets/moonworks/lunara-aesthetic-image-variations/resolve/main/data/train-00000-of-00017.parquet

run-example-mteb-1:
	uv run sqlexplore https://huggingface.co/datasets/mteb/tweet_sentiment_extraction/resolve/main/data/train-00000-of-00001.parquet

run-example-pipe-1:
	ps aux | uv run sqlexplore

run-example-pipe-2:
	ps aux | uv run sqlexplore --execute "SELECT * FROM data WHERE line ILIKE '%python%' LIMIT 100"

run-as-tool:
	uv run sqlexplore data/example.parquet

run-as-docker:
	docker compose run --rm app gnaf.parquet

docker-build:
	docker compose build --no-cache

test:
	uv run pytest -vv --capture=no --no-cov tests

test-selected:
	uv run pytest -vv --capture=no --no-cov tests -k "test_main_rejects_execute_and_query_file_together"

test-cov:
	uv run pytest -vv --capture=no --cov-report=term-missing --cov-report=html tests

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
	rm -rf dist
	rm -rf build
	rm -rf sqlexplore.egg-info
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
