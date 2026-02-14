# AGENTS Guidelines

This file contains guidelines for AI agents to follow when writing code in this project.

## Python Coding Guidelines

- Always be extremely concise. Sacrifice grammar for the sake of being concise.
- Write simple, clean and minimal code.
- Don't over complicate or over engineer solutions.
- Keep sensitive information in `.env` files.
- Keep functions small and focused on a single responsibility.
- Use type hints for all function parameters and return values.
- Provide meaningful error messages.
- Keep README.md up to date.
- Document all configuration options.
- Do not over-engineer solutions - strive for simplicity and maintainability while being efficient.
- Keep docstrings minimal, prefer the code to speak for itself.
- Leverage `async` and `await` for I/O-bound operations to maximize concurrency.
- Leverage `asyncio` as an event-loop framework when appropriate.
- Apply decorator `@functools.cache` function caching where appropriate.
- For any pandas data pipelines, prefer chainable functions with `DataFrame.pipe(func, ...)`.
- Use f-strings for string formatting, and prefer the f"{var=}" syntax to show the variable name and its value.
- A functional programming approach is preferred over an object-oriented one. Use classes when appropriate.
- Use Pydantic models when appropriate.

## Critical Thinking

- Fix root cause (not band-aid).
- If you are unsure: read more code; if still stuck, ask with short options.
- Prefer correct, maintainable implementations over clever, complex ones.
- If a popular, well tested library exists for a task, use it instead of reinventing the wheel.
- Be careful with secrets - do not expose them to the world (e.g. in logs, comments, git, etc.).

## Project Structure

- Package management is via `uv`.
- Manage dependencies via `pyproject.toml`, with dev dependencies in the `[dependency-groups]` section.
- Keep source code in a `./src/<package_name>/` directory.
- Place tests in a `./tests/<package_name>/` directory.
- Keep configuration files in the root directory.
- See below for more details on testing, linting, and type checking.
- See `Makefile` for additional project management commands.

## Tests

- Use `pytest` for testing
- Follow the naming convention: test_*.py
- Use fixtures for test setup and teardown
- Place test files in `./tests/<package_name>/`

## Linting

- Use `ruff` for formatting and linting
- Run `uv run ruff format .` to format the code
- Run `uv run ruff check . --fix` to check the code and fix any issues

## Type Checking

- Use `pyright` for type checking
- Run `uv run pyright` to type check the code
