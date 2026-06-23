"""Dagster asset definitions for the CTS-1 data pipeline.

Asset graph:
  satnogs_observations
        │
  downloaded_audio          (observation audio bytes → DB)
        │
  demodulated_frames        (gr_satellites runs → DB)
        │
  decoded_telemetry         (hex → field/value pairs → DB)

All assets are partitioned by day. A single run materializes one calendar day
of observations, so the pipeline is naturally incremental.
"""

from datetime import datetime

import polars as pl
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    Definitions,
    Output,
    ResourceDefinition,
    asset,
    define_asset_job,  # pyright: ignore[reportUnknownVariableType]
)
from loguru import logger

from cts1_data_pipeline.database.repository import (
    get_observation_row,
    insert_decoded_fields,
    insert_demod_frames,
    make_engine,
    make_session_factory,
    session_scope,
    upsert_observation,
)
from cts1_data_pipeline.database.schema import (
    DecodedFieldRow,
    DemodFrameRow,
    ObservationRow,
)
from cts1_data_pipeline.decoding.decoder import decode_frames
from cts1_data_pipeline.demodulation.runner import DemodRunner
from cts1_data_pipeline.models import AudioFile, DataOrigin, DemodAlgorithm, DemodResult
from cts1_data_pipeline.satnogs.client import SatnogsClient
from cts1_data_pipeline.settings import Settings

# ---------------------------------------------------------------------------
# Partition definition
# ---------------------------------------------------------------------------

# Adjust start_date to the satellite's first observation date.
daily_partitions = DailyPartitionsDefinition(start_date="2026-05-01")


# ---------------------------------------------------------------------------
# Resources
# ---------------------------------------------------------------------------


def _make_settings() -> Settings:
    return Settings.from_env()


settings_resource = ResourceDefinition.hardcoded_resource(
    _make_settings(), description="Pipeline settings from environment"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _window(context: AssetExecutionContext) -> tuple[datetime, datetime]:
    """Return (start, end) for the current daily partition as naive UTC datetimes."""
    w = context.partition_time_window
    return w.start.replace(tzinfo=None), w.end.replace(tzinfo=None)


# ---------------------------------------------------------------------------
# Asset: fetch and store observations for one day
# ---------------------------------------------------------------------------


@asset(
    partitions_def=daily_partitions,
    required_resource_keys={"settings"},
)
def satnogs_observations(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """Fetch SatNOGS observations for the partition day and persist to DB."""
    window_start, window_end = _window(context)
    settings: Settings = context.resources.settings  # type: ignore[attr-defined]
    client = SatnogsClient(settings)
    engine = make_engine(settings)
    factory = make_session_factory(engine)

    observations = client.fetch_all_observations(start=window_start, end=window_end)
    upserted = 0
    skipped = 0

    with session_scope(factory) as session:
        for obs in observations:
            row = get_observation_row(session, obs.observation_id)
            if row is not None and row.audio_data is not None:
                skipped += 1
                continue
            upsert_observation(session, obs, audio=None)
            upserted += 1

    logger.info("upserted={} skipped={}", upserted, skipped)

    df = pl.DataFrame(
        {
            "observation_id": [o.observation_id for o in observations],
            "satellite_norad": [o.satellite_norad for o in observations],
            "has_audio": [o.audio_url is not None for o in observations],
            "status": [o.status for o in observations],
            "vetted_status": [o.vetted_status for o in observations],
        }
    )

    context.add_output_metadata(
        {
            "total_observations": len(observations),
            "with_audio": int(df["has_audio"].sum()),
        }
    )
    return Output(df)


# ---------------------------------------------------------------------------
# Asset: download audio for one day's observations
# ---------------------------------------------------------------------------


@asset(
    partitions_def=daily_partitions,
    required_resource_keys={"settings"},
    deps=[satnogs_observations],
)
def downloaded_audio(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """Download audio for all observations in the partition day that lack it."""
    window_start, window_end = _window(context)
    settings: Settings = context.resources.settings  # type: ignore[attr-defined]
    client = SatnogsClient(settings)
    engine = make_engine(settings)
    factory = make_session_factory(engine)

    with session_scope(factory) as session:
        observations_in_db: list[ObservationRow] = (
            session.query(ObservationRow)
            .filter_by(origin=DataOrigin.SATNOGS.value)
            .filter(
                ObservationRow.start_utc >= window_start,
                ObservationRow.start_utc < window_end,
                ObservationRow.audio_data.is_(None),
            )
            .all()
        )
        external_ids_needing_audio = {row.external_id for row in observations_in_db}

    if not external_ids_needing_audio:
        logger.info(
            "All observations for {} already have audio.", context.partition_key
        )
        return Output(pl.DataFrame({"observation_id": [], "downloaded": []}))

    # Re-fetch the window to get audio URLs (not stored in DB by design).
    all_observations = client.fetch_all_observations(start=window_start, end=window_end)
    to_download = [
        obs
        for obs in all_observations
        if obs.observation_id in external_ids_needing_audio
    ]

    downloaded: list[int] = []
    failed: list[int] = []

    for obs in to_download:
        audio = client.download_audio(obs)
        if audio is None:
            failed.append(obs.observation_id)
            continue
        with session_scope(factory) as session:
            upsert_observation(session, obs, audio=audio)
        downloaded.append(obs.observation_id)

    context.add_output_metadata({"downloaded": len(downloaded), "failed": len(failed)})

    return Output(
        pl.DataFrame(
            {
                "observation_id": downloaded + failed,
                "downloaded": [True] * len(downloaded) + [False] * len(failed),
            }
        )
    )


# ---------------------------------------------------------------------------
# Asset: demodulate audio for one day
# ---------------------------------------------------------------------------


@asset(
    partitions_def=daily_partitions,
    required_resource_keys={"settings"},
    deps=[downloaded_audio],
)
def demodulated_frames(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """Run gr_satellites on every observation in the partition day that has audio."""
    window_start, window_end = _window(context)
    settings: Settings = context.resources.settings  # type: ignore[attr-defined]
    engine = make_engine(settings)
    factory = make_session_factory(engine)

    with session_scope(factory) as session:
        obs_rows: list[ObservationRow] = (
            session.query(ObservationRow)
            .filter_by(origin=DataOrigin.SATNOGS.value)
            .filter(
                ObservationRow.start_utc >= window_start,
                ObservationRow.start_utc < window_end,
                ObservationRow.audio_data.isnot(None),
            )
            .all()
        )
        obs_ids = {row.id for row in obs_rows}
        obs_with_frames: set[int] = {
            row.observation_id
            for row in session.query(DemodFrameRow.observation_id)
            .filter(DemodFrameRow.observation_id.in_(obs_ids))
            .distinct()
            .all()
        }
        to_demod = [row for row in obs_rows if row.id not in obs_with_frames]

    if not to_demod:
        logger.info("No new observations to demodulate for {}.", context.partition_key)
        return Output(pl.DataFrame({"observation_id": [], "frame_count": []}))

    audio_files = [
        AudioFile(
            observation_id=row.external_id,
            content_type=row.audio_content_type or "audio/ogg",
            data=row.audio_data,  # type: ignore[arg-type]
        )
        for row in to_demod
    ]
    ext_to_db_id = {row.external_id: row.id for row in to_demod}

    runner = DemodRunner(max_workers=settings.max_parallel_demod)
    batches = runner.run_all(audio_files)

    frame_counts: dict[int, int] = {}
    with session_scope(factory) as session:
        for batch in batches:
            db_obs_id = ext_to_db_id.get(batch.observation_id)
            if db_obs_id is None:
                logger.warning("No DB row for obs ext_id={}", batch.observation_id)
                continue
            if batch.frames:
                insert_demod_frames(session, db_obs_id, batch.frames)
            frame_counts[batch.observation_id] = len(batch.frames)

    context.add_output_metadata(
        {
            "observations_processed": len(batches),
            "total_frames": sum(frame_counts.values()),
        }
    )

    return Output(
        pl.DataFrame(
            {
                "observation_id": list(frame_counts.keys()),
                "frame_count": list(frame_counts.values()),
            }
        )
    )


# ---------------------------------------------------------------------------
# Asset: decode frames for one day
# ---------------------------------------------------------------------------


@asset(
    partitions_def=daily_partitions,
    required_resource_keys={"settings"},
    deps=[demodulated_frames],
)
def decoded_telemetry(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """Decode all demod frames for the partition day into named telemetry fields."""
    window_start, window_end = _window(context)
    settings: Settings = context.resources.settings  # type: ignore[attr-defined]
    engine = make_engine(settings)
    factory = make_session_factory(engine)

    with session_scope(factory) as session:
        obs_rows: list[ObservationRow] = (
            session.query(ObservationRow)
            .filter(
                ObservationRow.start_utc >= window_start,
                ObservationRow.start_utc < window_end,
            )
            .all()
        )
        partition_obs_ids = {row.id for row in obs_rows}

        all_frames: list[DemodFrameRow] = (
            session.query(DemodFrameRow)
            .filter(DemodFrameRow.observation_id.in_(partition_obs_ids))
            .all()
        )
        partition_frame_ids = {f.id for f in all_frames}

        decoded_frame_ids: set[int] = {
            row.demod_frame_id
            for row in session.query(DecodedFieldRow.demod_frame_id)
            .filter(DecodedFieldRow.demod_frame_id.in_(partition_frame_ids))
            .distinct()
            .all()
        }
        pending = [f for f in all_frames if f.id not in decoded_frame_ids]

    if not pending:
        logger.info("No new frames to decode for {}.", context.partition_key)
        return Output(pl.DataFrame({"field_name": [], "count": []}))

    db_frame_id_map: dict[tuple[int, str], int] = {}
    for frame_row in pending:
        obs_row = next((o for o in obs_rows if o.id == frame_row.observation_id), None)
        if obs_row is None:
            continue
        db_frame_id_map[(obs_row.external_id, frame_row.hex_data)] = frame_row.id

    db_obs_id_map: dict[int, int] = {row.external_id: row.id for row in obs_rows}

    domain_frames: list[DemodResult] = []
    for frame_row in pending:
        obs_row = next((o for o in obs_rows if o.id == frame_row.observation_id), None)
        if obs_row is None:
            continue
        try:
            algo = DemodAlgorithm(frame_row.algorithm)
        except ValueError:
            algo = DemodAlgorithm.GR_SATELLITES_WAV
        domain_frames.append(
            DemodResult(
                observation_id=obs_row.external_id,
                algorithm=algo,
                timestamp_utc=frame_row.timestamp_utc,
                hex_data=frame_row.hex_data,
                origin=DataOrigin.SATNOGS,
            )
        )

    decoded = decode_frames(domain_frames, db_frame_id_map, db_obs_id_map)

    with session_scope(factory) as session:
        insert_decoded_fields(session, decoded)

    field_counts: dict[str, int] = {}
    for field in decoded:
        field_counts[field.field_name] = field_counts.get(field.field_name, 0) + 1

    context.add_output_metadata({"total_decoded_fields": len(decoded)})

    return Output(
        pl.DataFrame(
            {
                "field_name": list(field_counts.keys()),
                "count": list(field_counts.values()),
            }
        )
    )


# ---------------------------------------------------------------------------
# Job and Definitions
# ---------------------------------------------------------------------------

cts1_pipeline_job = define_asset_job(
    name="cts1_pipeline_job",
    selection=[
        satnogs_observations,
        downloaded_audio,
        demodulated_frames,
        decoded_telemetry,
    ],
    partitions_def=daily_partitions,
)

defs = Definitions(
    assets=[
        satnogs_observations,
        downloaded_audio,
        demodulated_frames,
        decoded_telemetry,
    ],
    jobs=[cts1_pipeline_job],
    resources={"settings": settings_resource},
)
