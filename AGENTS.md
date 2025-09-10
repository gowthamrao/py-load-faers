# Agent Instructions for py-load-faers

This document provides instructions for AI agents working on the `py-load-faers` codebase.

## 1. Code Quality and Style

- All code must be fully type-hinted and pass `mypy --strict`.
- Code must be formatted with `black`.
- Code must pass linting with `ruff`.
- Follow the requirements outlined in the `FRD` (Functional Requirements Document).

## 2. Project Structure

- The core logic is located in `py_load_faers/`.
- Database-specific implementations should be in sub-packages (e.g., `py_load_faers_postgres/`).
- All new features should have corresponding tests in the `tests/` directory.

## 3. Testing

- Unit tests should be written with `pytest`.
- Integration tests use `testcontainers` and require a running Docker daemon. The user running the tests must have permissions to access the Docker socket (e.g., by being in the `docker` group).
- Before submitting, ensure all tests pass by running `poetry run pytest`.

## 4. Commits and Pull Requests

- Commit messages should be descriptive and follow conventional commit standards.
- Pull requests should be small and focused on a single feature or bug fix.
- Ensure the PR description clearly explains the changes and references the relevant issue or requirement from the FRD.
