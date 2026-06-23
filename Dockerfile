FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Store venv outside of the repo so that volume mount doesn't break
# the venv inside and outside the container.
ENV UV_PROJECT_ENVIRONMENT=/venv

ENV PATH="/venv/bin:$PATH"

COPY . /app

RUN uv sync --frozen --no-dev

EXPOSE 3000
