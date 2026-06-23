FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml uv.lock uv.toml ./

RUN uv sync --frozen --no-dev --no-install-project

COPY src/ src/
COPY migrations/ migrations/
COPY alembic.ini .
COPY config/ config/

RUN uv sync --frozen --no-dev

EXPOSE 3000
