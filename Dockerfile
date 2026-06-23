FROM python:3.11-slim

# gr_satellites system deps (GNU Radio, etc.) must be installed separately
# on the host or via a custom base image.  This Dockerfile covers the Python
# pipeline only.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml .
COPY cts1_data_pipeline/ cts1_data_pipeline/
COPY migrations/ migrations/
COPY alembic.ini .
COPY config/ config/

RUN pip install --no-cache-dir -e .

# Expose Dagster webserver port
EXPOSE 3000
