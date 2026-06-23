# CTS-1 Data Pipeline

Data processing pipeline for CTS-SAT-1 data

## Overview

This project pulls overpass downlink logs from the public SatNOGS network, performs demodulation, and stores the results in a clean Postgres SQL database.

This project does not use nor present any secret or internal-only data.

## Architecture

```
SatNOGS Network API
        │
  satnogs_observations   ← fetch + store observation metadata
        │
  downloaded_audio       ← download audio bytes → store in DB (no local files)
        │
  demodulated_frames     ← gr_satellites (parallel subprocesses via threads)
        │
  decoded_telemetry      ← hex → field/value pairs
```

All four steps are Dagster software-defined assets.  Re-materialise individual
assets to re-run only the part you need (e.g., re-run demodulation with a new
algorithm without re-downloading audio).

## Configuration

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
# edit .env
```

Key variables:

| Variable | Description |
|---|---|
| `SATNOGS_NETWORK_API_TOKEN` | Network API token (required). From https://network.satnogs.org/accounts/api-auth-token/ |
| `SATNOGS_DB_API_TOKEN` | DB API token (not currently used). From https://db.satnogs.org/accounts/api-auth-token/ |
| `SATELLITE_NORAD` | NORAD ID to ingest (default: 69015) |
| `DATABASE_URL` | SQLAlchemy URL for PostgreSQL |
| `MAX_PARALLEL_DEMOD` | Concurrent `gr_satellites` subprocesses (default: 4) |

---

## Running

### 1. Start PostgreSQL

```bash
docker compose up postgres -d
```

### 2. Run database migrations

```bash
export $(cat .env | xargs)
uv run alembic upgrade head
```

### 3. Start Dagster

```bash
uv run dagster-webserver -w workspace.yaml
```

Open http://localhost:3000, then materialise the assets in order, or run the
`cts1_pipeline_job` job to execute all four steps end-to-end.

### All-in-one (Docker)

```bash
docker compose up
```

The webserver is available at http://localhost:3000.

## Database schema

| Table | Purpose |
|---|---|
| `observations` | One row per SatNOGS observation. Audio stored as `bytea`. |
| `demod_frames` | One row per decoded hex frame, linked to its observation. |
| `decoded_fields` | One row per telemetry field, linked to its demod frame. |

All tables carry an `origin` column (`"satnogs"` for SatNOGS data) for future
extensibility with non-SatNOGS data sources.

---

## Development

```bash
# Lint
ruff check .

# Type check
pyright

# Tests
pytest
```
