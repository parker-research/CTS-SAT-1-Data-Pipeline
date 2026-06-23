"""Telemetry decoder for CTS-1 / FRONTIERSAT hex frames.

Converts raw hexdump frames (from gr_satellites) into named field/value pairs.

The FRONTIERSAT telemetry format uses the AX100 ASM+Golay framing with a
CCSDS scrambler. The hex payload after framing/descrambling is the application
layer.  This module provides a best-effort parser — extend it as the FRONTIERSAT
ICD is further documented.
"""

from dataclasses import dataclass

import logging

log = logging.getLogger(__name__)

from cts1_data_pipeline.models import DataOrigin, DecodedFrame, DemodResult


@dataclass(frozen=True)
class _FieldSpec:
    name: str
    byte_offset: int
    byte_length: int
    scale: float = 1.0
    unit: str = ""


# ---------------------------------------------------------------------------
# Minimal FRONTIERSAT telemetry field map
# Replace / extend with the real ICD byte offsets.
# ---------------------------------------------------------------------------
_FIELD_SPECS: list[_FieldSpec] = [
    _FieldSpec(
        "eps_battery_voltage_mv", byte_offset=0, byte_length=2, scale=1.0, unit="mV"
    ),
    _FieldSpec(
        "eps_battery_current_ma", byte_offset=2, byte_length=2, scale=1.0, unit="mA"
    ),
    _FieldSpec("obc_uptime_s", byte_offset=4, byte_length=4, scale=1.0, unit="s"),
    _FieldSpec("obc_temp_raw", byte_offset=8, byte_length=2, scale=0.1, unit="°C"),
]


def _decode_frame(hex_data: str) -> dict[str, str]:
    """Attempt to decode known fields from a hex payload.

    Returns a dict of field_name → string representation of the value.
    Unknown bytes are ignored; fields that cannot be extracted are omitted.
    """
    try:
        raw = bytes.fromhex(hex_data.replace(" ", ""))
    except ValueError:
        log.warning("Could not parse hex payload: %r", hex_data[:40])
        return {}

    fields: dict[str, str] = {}
    for spec in _FIELD_SPECS:
        end = spec.byte_offset + spec.byte_length
        if end > len(raw):
            continue
        chunk = raw[spec.byte_offset : end]
        int_val = int.from_bytes(chunk, byteorder="big", signed=False)
        float_val = int_val * spec.scale
        formatted = f"{float_val:.4g} {spec.unit}".strip()
        fields[spec.name] = formatted

    # Always store the raw hex as a field for reference
    fields["raw_hex"] = hex_data.replace(" ", "").upper()

    return fields


def decode_frames(
    frames: list[DemodResult],
    db_frame_id_map: dict[
        tuple[int, str], int
    ],  # (obs_id, hex_data) → db demod_frame id
    db_obs_id_map: dict[int, int],  # satnogs obs_id → db observations.id
) -> list[DecodedFrame]:
    """Decode all frames into DecodedFrame records.

    Args:
        frames: Raw DemodResult list.
        db_frame_id_map: Maps (observation_id, hex_data) → DB demod_frame.id.
        db_obs_id_map: Maps SatNOGS observation_id → DB observations.id.

    Returns:
        Flat list of DecodedFrame for all successfully decoded fields.

    """
    decoded: list[DecodedFrame] = []

    for frame in frames:
        demod_frame_id = db_frame_id_map.get((frame.observation_id, frame.hex_data))
        db_obs_id = db_obs_id_map.get(frame.observation_id)
        if demod_frame_id is None or db_obs_id is None:
            log.warning(
                "No DB ID mapping for obs=%s — skipping decoding.", frame.observation_id
            )
            continue

        fields = _decode_frame(frame.hex_data)
        if not fields:
            log.debug("obs=%s frame produced no decoded fields.", frame.observation_id)
            continue

        for name, value in fields.items():
            decoded.append(
                DecodedFrame(
                    demod_frame_id=demod_frame_id,
                    observation_id=db_obs_id,
                    timestamp_utc=frame.timestamp_utc,
                    field_name=name,
                    field_value=value,
                    origin=DataOrigin.SATNOGS,
                )
            )

    log.info(
        "Decoded %d telemetry field records from %d frames.", len(decoded), len(frames)
    )
    return decoded
