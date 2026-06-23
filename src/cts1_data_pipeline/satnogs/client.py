"""SatNOGS Network API client.

Fetches observations and downloads audio for a given satellite NORAD ID.
All network I/O uses httpx with tenacity retries.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any

import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from cts1_data_pipeline.models import AudioFile, SatnogsObservation

if TYPE_CHECKING:
    from cts1_data_pipeline.settings import Settings


def _parse_observation(raw: dict[str, Any]) -> SatnogsObservation:
    """Parse one observation dict from the SatNOGS API into a dataclass."""

    def _dt(val: str | None) -> datetime | None:
        if val is None:
            return None
        return datetime.fromisoformat(val.rstrip("Z"))

    start = _dt(raw.get("start"))
    end = _dt(raw.get("end"))
    if start is None or end is None:
        raise ValueError(
            f"Observation {raw.get('id')} is missing start/end timestamps."
        )

    return SatnogsObservation(
        observation_id=int(raw["id"]),
        satellite_norad=int(raw.get("norad_cat_id", 0)),
        start=start,
        end=end,
        transmitter=raw.get("transmitter", ""),
        status=raw.get("status", ""),
        vetted_status=raw.get("vetted_status", ""),
        audio_url=raw.get("payload") or None,
        waterfall_url=raw.get("waterfall") or None,
        tle_line1=raw.get("tle0") or None,
        tle_line2=raw.get("tle1") or None,
    )


class SatnogsClient:
    """Thin wrapper around the SatNOGS Network REST API."""

    def __init__(self, settings: Settings) -> None:
        self._base = settings.satnogs_network_base_url.rstrip("/")
        self._headers = {
            "Authorization": f"Token {settings.satnogs_network_api_token}",
        }
        self._norad = settings.satellite_norad

    # ------------------------------------------------------------------
    # Observation listing
    # ------------------------------------------------------------------

    def fetch_all_observations(self) -> list[SatnogsObservation]:
        """Paginate through all observations for the configured NORAD ID."""
        observations: list[SatnogsObservation] = []
        url: str | None = (
            f"{self._base}/observations/"
            f"?satellite__norad_cat_id={self._norad}&format=json"
        )

        with httpx.Client(headers=self._headers, timeout=30) as client:
            while url is not None:
                logger.debug("GET {}", url)
                response = self._get(client, url)
                data = response.json()

                # The API returns either a list or a paginated dict
                if isinstance(data, list):
                    results = data
                    url = None
                else:
                    results = data.get("results", [])
                    url = data.get("next")

                for raw in results:
                    try:
                        obs = _parse_observation(raw)
                        observations.append(obs)
                    except (KeyError, ValueError) as exc:
                        logger.warning("Skipping malformed observation: {}", exc)

        logger.info(
            "Fetched {} observations for NORAD {}", len(observations), self._norad
        )
        return observations

    # ------------------------------------------------------------------
    # Audio download
    # ------------------------------------------------------------------

    def download_audio(self, observation: SatnogsObservation) -> AudioFile | None:
        """Download the audio payload for an observation.

        Returns None if no audio URL exists or the download fails.
        """
        if observation.audio_url is None:
            logger.debug(
                "Observation {} has no audio URL — skipping.",
                observation.observation_id,
            )
            return None

        with httpx.Client(
            headers=self._headers, timeout=120, follow_redirects=True
        ) as client:
            try:
                audio_bytes = self._download(client, observation.audio_url)
            except Exception as exc:
                logger.warning(
                    "Failed to download audio for observation {}: {}",
                    observation.observation_id,
                    exc,
                )
                return None

        content_type = "audio/ogg"  # SatNOGS typically serves ogg
        logger.info(
            "Downloaded {:.1f} KB for observation {}",
            len(audio_bytes) / 1024,
            observation.observation_id,
        )
        return AudioFile(
            observation_id=observation.observation_id,
            content_type=content_type,
            data=audio_bytes,
        )

    # ------------------------------------------------------------------
    # Internal helpers with retry
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30)
    )
    def _get(self, client: httpx.Client, url: str) -> httpx.Response:
        response = client.get(url)
        response.raise_for_status()
        return response

    @retry(
        stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30)
    )
    def _download(self, client: httpx.Client, url: str) -> bytes:
        response = client.get(url)
        response.raise_for_status()
        return response.content
