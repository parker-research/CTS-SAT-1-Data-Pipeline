default:
    just --list

check:
    uv run pyright .
    uv run ruff check .
    uv run ruff format .
    uv run pytest
