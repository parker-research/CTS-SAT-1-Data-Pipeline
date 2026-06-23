"""Pipeline configuration loaded from environment variables."""

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """All runtime configuration for the pipeline.

    Populate via environment variables (e.g. a .env file + export, or Docker env).
    """

    # --- SatNOGS ---
    # Network API token — used to authenticate against network.satnogs.org
    # Required for fetching observations and downloading audio.
    satnogs_network_api_token: str

    # DB API token — used against db.satnogs.org (satellite metadata, TLEs, etc.)
    # Not currently needed by the pipeline but reserved for future use.
    satnogs_db_api_token: str | None

    # NORAD ID of the satellite to ingest
    satellite_norad: int

    # --- Database ---
    database_url: str  # e.g. postgresql+psycopg2://user:pass@localhost:5432/cts1

    # --- gr_satellites ---
    gr_satellites_config: Path  # absolute path to the .yml satellite definition
    max_parallel_demod: int  # how many gr_satellites subprocesses to run at once

    # --- SatNOGS Network base URL (override for testing) ---
    satnogs_network_base_url: str = "https://network.satnogs.org/api"

    @classmethod
    def from_env(cls) -> Settings:
        """Build Settings from environment variables, raising on missing required ones."""

        def _require(key: str) -> str:
            value = os.environ.get(key)
            if not value:
                raise OSError(f"Required environment variable {key!r} is not set.")
            return value

        return cls(
            satnogs_network_api_token=_require("SATNOGS_NETWORK_API_TOKEN"),
            satnogs_db_api_token=os.environ.get("SATNOGS_DB_API_TOKEN"),
            satellite_norad=int(_require("SATELLITE_NORAD")),
            database_url=_require("DATABASE_URL"),
            gr_satellites_config=Path(_require("GR_SATELLITES_CONFIG")),
            max_parallel_demod=int(os.environ.get("MAX_PARALLEL_DEMOD", "4")),
            satnogs_network_base_url=os.environ.get(
                "SATNOGS_NETWORK_BASE_URL", "https://network.satnogs.org/api"
            ),
        )
