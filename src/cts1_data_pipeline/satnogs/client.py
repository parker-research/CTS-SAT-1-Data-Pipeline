"""SatNOGS Network API client."""

import logging
import re
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

from cts1_data_pipeline.models import SatnogsObservation
from cts1_data_pipeline.settings import Settings

log = logging.getLogger(__name__)

_LINK_NEXT_RE = re.compile(r'<([^>]+)>;\s*rel="next"')
_DATETIME_FMT = "%Y-%m-%dT%H:%M:%S"


def _next_url_from_headers(headers: Mapping[str, str]) -> str | None:
    """Extract the next-page URL from a Link header, or None if absent."""
    link = headers.get("Link", "")
    m = _LINK_NEXT_RE.search(link)
    return m.group(1) if m else None


def _parse_observation(raw: dict[str, Any]) -> SatnogsObservation:
    """Parse one observation dict from the SatNOGS API into a dataclass."""

    def _dt(val: str | None) -> datetime | None:
        if val is None:
            return None
        return datetime.fromisoformat(val.rstrip("Z"))

    start = _dt(raw.get("start"))
    end = _dt(raw.get("end"))
    if start is None or end is None:
        msg = f"Observation {raw.get('id')} is missing start/end timestamps."
        raise ValueError(msg)

    return SatnogsObservation(
        observation_id=int(raw["id"]),
        satellite_norad=int(raw.get("norad_cat_id", 0)),
        start=start,
        end=end,
        transmitter=raw.get("transmitter", ""),
        status=raw.get("status", ""),
        vetted_status=raw.get("vetted_status", ""),
        audio_url=raw.get("payload"),
        waterfall_url=raw.get("waterfall"),
        tle_line0=raw.get("tle0"),
        tle_line1=raw.get("tle1"),
        tle_line2=raw.get("tle2"),
    )


class SatnogsClient:
    """Thin wrapper around the SatNOGS Network REST API."""

    def __init__(self, settings: Settings) -> None:
        """Construct a new SatnogsClient."""
        self._base = settings.satnogs_network_base_url.rstrip("/")
        self._headers = {"Authorization": f"Token {settings.satnogs_network_api_key}"}
        self._norad = settings.satellite_norad

    # ------------------------------------------------------------------
    # Observation listing
    # ------------------------------------------------------------------

    def _iter_observation_pages(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield pages of observations, following Link-header cursor pagination.

        Args:
            start: Only return observations whose start time is >= this value.
            end: Only return observations whose start time is < this value.

        """
        url: str | None = f"{self._base}/observations/"
        params: dict[str, Any] = {
            "norad_cat_id": self._norad,
            "format": "json",
            "page_size": 100,
        }
        if start is not None:
            params["start"] = start.astimezone(UTC).strftime(_DATETIME_FMT)
        if end is not None:
            params["start__lt"] = end.astimezone(UTC).strftime(_DATETIME_FMT)

        while url is not None:
            log.debug("GET %s", url)
            r = requests.get(url, params=params, headers=self._headers, timeout=15)
            r.raise_for_status()
            page: list[dict[str, Any]] = r.json()
            assert isinstance(page, list), f"expected list, got {type(page)}"  # noqa: S101
            if page:
                yield page
            url = _next_url_from_headers(r.headers)
            params = {}  # cursor URL already encodes all query params

    def fetch_all_observations(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[SatnogsObservation]:
        """Paginate through all observations for the configured NORAD ID.

        Args:
            start: Only return observations that started at or after this time (UTC).
            end: Only return observations that started before this time (UTC).

        """
        observations: list[SatnogsObservation] = []
        for page in self._iter_observation_pages(start=start, end=end):
            for raw in page:
                try:
                    observations.append(_parse_observation(raw))
                except (KeyError, ValueError) as exc:
                    log.warning("Skipping malformed observation: %s", exc)
        log.info("Fetched %d observations for NORAD %s", len(observations), self._norad)
        return observations

    # ------------------------------------------------------------------
    # Audio download
    # ------------------------------------------------------------------

    def download_audio_to_file(self, audio_url: str, dest: Path) -> bool:
        """Download the audio at *audio_url* and write it to *dest*.

        Returns True on success, False on HTTP or I/O errors.
        """
        try:
            r = requests.get(audio_url, headers=self._headers, timeout=60, stream=True)
            r.raise_for_status()
        except requests.RequestException as exc:
            log.warning("Failed to download audio from %s: %s", audio_url, exc)
            return False

        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            with dest.open("wb") as fh:
                for chunk in r.iter_content(chunk_size=65536):
                    fh.write(chunk)
        except OSError as exc:
            log.warning("Failed to write audio to %s: %s", dest, exc)
            return False

        log.debug("Downloaded audio → %s (%d bytes)", dest, dest.stat().st_size)
        return True
