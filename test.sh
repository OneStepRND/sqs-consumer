#!/usr/bin/env bash
set -euo pipefail
uv sync --dev
uv run ruff check
uv run ruff format --check
uv run pyright
uv run pytest test