"""Core domain dataclasses shared across the pipeline."""

import enum
from dataclasses import dataclass, field
from datetime import datetime


class DataOrigin(enum.StrEnum):
    """Source system from which an observation or frame originated."""

    SATNOGS = "satnogs"


class DemodAlgorithm(enum.StrEnum):
    """Demodulation algorithm / tool variant used to process audio."""

    GR_SATELLITES_WAV = "gr_satellites_wav"
    GR_SATELLITES_IQ = "gr_satellites_iq"


@dataclass(frozen=True)
class SatnogsObservation:
    """Raw observation record returned by the SatNOGS Network API."""

    observation_id: int
    satellite_norad: int
    start: datetime
    end: datetime
    transmitter: str
    status: str
    vetted_status: str
    audio_url: str | None
    waterfall_url: str | None
    tle_line1: str | None
    tle_line2: str | None


@dataclass(frozen=True)
class AudioFile:
    """In-memory audio payload fetched from SatNOGS."""

    observation_id: int
    content_type: str  # e.g. "audio/ogg"
    data: bytes


@dataclass(frozen=True)
class DemodResult:
    """A single demodulated hex frame produced by gr_satellites."""

    observation_id: int
    algorithm: DemodAlgorithm
    timestamp_utc: datetime
    hex_data: str
    origin: DataOrigin = DataOrigin.SATNOGS


@dataclass
class DemodBatch:
    """Collection of demodulated frames from one observation run."""

    observation_id: int
    algorithm: DemodAlgorithm
    frames: list[DemodResult] = field(default_factory=list[DemodResult])
    returncode: int = 0
    stderr: str = ""


@dataclass(frozen=True)
class DecodedFrame:
    """Decoded telemetry tied back to its raw demod frame."""

    demod_frame_id: int  # FK to demod_frames.id
    observation_id: int
    timestamp_utc: datetime
    field_name: str
    field_value: str
    origin: DataOrigin = DataOrigin.SATNOGS
