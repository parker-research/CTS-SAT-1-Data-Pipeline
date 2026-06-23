"""gr_satellites subprocess runner.

Runs gr_satellites in parallel subprocesses (one per observation audio file)
using a thread pool.  Parses the hexdump output into DemodResult dataclasses.
"""

import logging
import re
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from cts1_data_pipeline.models import (
    AudioFile,
    DataOrigin,
    DemodAlgorithm,
    DemodBatch,
    DemodResult,
)

log = logging.getLogger(__name__)

_SATELLITE_CONFIG = Path(__file__).parent / "frontiersat.yml"

# gr_satellites hexdump line format (from --hexdump output):
#   2024-03-15 12:34:56.789 [hexdump] XX XX XX XX ...
_HEXDUMP_RE = re.compile(
    r"(?P<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?)"
    r"\s+\[hexdump\]\s+(?P<hex>[0-9A-Fa-f ]+)"
)


def _parse_hexdump_line(line: str) -> tuple[datetime, str] | None:
    """Parse one hexdump output line into (timestamp, hex_string) or None."""
    m = _HEXDUMP_RE.search(line)
    if not m:
        return None
    ts_str = m.group("ts")
    hex_data = m.group("hex").strip()
    # Try both with and without sub-seconds
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            ts = datetime.strptime(ts_str, fmt).replace(tzinfo=UTC)

        except ValueError:
            continue
        else:
            return ts, hex_data
    return None


def _run_gr_satellites_wav(
    audio_data: bytes,
    observation_id: int,
) -> DemodBatch:
    """Write audio to a temp file, run gr_satellites, parse hexdump output."""
    batch = DemodBatch(
        observation_id=observation_id,
        algorithm=DemodAlgorithm.GR_SATELLITES_WAV,
    )

    with tempfile.NamedTemporaryFile(suffix=".wav", delete=True) as tmp:
        tmp.write(audio_data)
        tmp.flush()
        tmp_path = tmp.name

        cmd = [
            "gr_satellites",
            str(_SATELLITE_CONFIG),
            "--hexdump",
            "--wavfile",
            tmp_path,
        ]
        log.debug("obs=%s cmd=%s", observation_id, " ".join(cmd))

        try:
            result = subprocess.run(  # noqa: PLW1510, S603
                cmd,
                capture_output=True,
                text=True,
                timeout=300,
            )
        except subprocess.TimeoutExpired:
            log.warning("obs=%s gr_satellites timed out", observation_id)
            batch.returncode = -1
            batch.stderr = "timeout"
            return batch
        except FileNotFoundError:
            log.exception("gr_satellites not found in PATH")
            batch.returncode = -2
            batch.stderr = "gr_satellites not found"
            return batch

    batch.returncode = result.returncode
    batch.stderr = result.stderr

    if result.returncode != 0:
        log.warning(
            "obs=%s gr_satellites exited %s: %s",
            observation_id,
            result.returncode,
            result.stderr[:200],
        )

    for line in result.stdout.splitlines():
        parsed = _parse_hexdump_line(line)
        if parsed is None:
            continue
        ts, hex_data = parsed
        batch.frames.append(
            DemodResult(
                observation_id=observation_id,
                algorithm=DemodAlgorithm.GR_SATELLITES_WAV,
                timestamp_utc=ts,
                hex_data=hex_data,
                origin=DataOrigin.SATNOGS,
            )
        )

    log.info(
        "obs=%s decoded %d frames (algorithm=%s)",
        observation_id,
        len(batch.frames),
        DemodAlgorithm.GR_SATELLITES_WAV.value,
    )
    return batch


@dataclass
class DemodRunner:
    """Parallelized demodulation runner."""

    max_workers: int = 4

    def run_all(self, audio_files: list[AudioFile]) -> list[DemodBatch]:
        """Run gr_satellites on all audio files in parallel, returning all batches."""
        if not audio_files:
            return []

        batches: list[DemodBatch] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {
                pool.submit(
                    _run_gr_satellites_wav,
                    af.data,
                    af.observation_id,
                ): af.observation_id
                for af in audio_files
            }
            for future in as_completed(futures):
                obs_id = futures[future]
                try:
                    batch = future.result()
                    batches.append(batch)
                except Exception:
                    log.exception("Unexpected error demodulating obs=%s", obs_id)

        return batches
