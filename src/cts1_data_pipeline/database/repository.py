"""Database engine, session factory, and repository-style helpers."""

import logging
from collections.abc import Generator
from contextlib import contextmanager

import sqlalchemy as sa
from sqlalchemy.orm import Session, sessionmaker

from cts1_data_pipeline.database.schema import (
    DecodedFieldRow,
    DemodFrameRow,
    ObservationRow,
)
from cts1_data_pipeline.models import (
    DataOrigin,
    DecodedFrame,
    DemodResult,
    SatnogsObservation,
)
from cts1_data_pipeline.settings import Settings

log = logging.getLogger(__name__)


def make_engine(settings: Settings) -> sa.Engine:
    """Create a SQLAlchemy engine from settings."""
    engine = sa.create_engine(settings.database_url, pool_pre_ping=True)
    log.info("Database engine created: %s", settings.database_url.split("@")[-1])
    return engine


def make_session_factory(engine: sa.Engine) -> sessionmaker[Session]:
    """Return a bound session factory."""
    return sessionmaker(bind=engine, expire_on_commit=False)


@contextmanager
def session_scope(factory: sessionmaker[Session]) -> Generator[Session]:
    """Context manager providing a transactional session."""
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Observation upsert
# ---------------------------------------------------------------------------


def upsert_observation(
    session: Session,
    obs: SatnogsObservation,
) -> ObservationRow:
    """Insert or update an observation row; return the persisted row."""
    existing = (
        session.query(ObservationRow)
        .filter_by(
            origin=DataOrigin.SATNOGS.value,
            external_id=obs.observation_id,
        )
        .one_or_none()
    )

    if existing is not None:
        row = existing
    else:
        row = ObservationRow(
            origin=DataOrigin.SATNOGS.value,
            external_id=obs.observation_id,
        )
        session.add(row)

    row.satellite_norad = obs.satellite_norad
    row.start_utc = obs.start
    row.end_utc = obs.end
    row.transmitter = obs.transmitter
    row.status = obs.status
    row.vetted_status = obs.vetted_status
    row.audio_url = obs.audio_url
    row.waterfall_url = obs.waterfall_url
    row.tle_line0 = obs.tle_line0
    row.tle_line1 = obs.tle_line1
    row.tle_line2 = obs.tle_line2

    session.flush()
    return row


def get_observation_row(session: Session, external_id: int) -> ObservationRow | None:
    """Look up an observation by its SatNOGS ID."""
    return (
        session.query(ObservationRow)
        .filter_by(origin=DataOrigin.SATNOGS.value, external_id=external_id)
        .one_or_none()
    )


# ---------------------------------------------------------------------------
# Demod frame insert
# ---------------------------------------------------------------------------


def insert_demod_frames(
    session: Session,
    db_obs_id: int,
    frames: list[DemodResult],
) -> list[DemodFrameRow]:
    """Bulk-insert demodulated frames and return the persisted rows."""
    rows = [
        DemodFrameRow(
            origin=f.origin.value,
            observation_id=db_obs_id,
            algorithm=f.algorithm.value,
            timestamp_utc=f.timestamp_utc,
            hex_data=f.hex_data,
        )
        for f in frames
    ]
    session.add_all(rows)
    session.flush()
    return rows


# ---------------------------------------------------------------------------
# Decoded field insert
# ---------------------------------------------------------------------------


def insert_decoded_fields(
    session: Session,
    fields: list[DecodedFrame],
) -> None:
    """Bulk-insert decoded telemetry fields."""
    rows = [
        DecodedFieldRow(
            origin=f.origin.value,
            demod_frame_id=f.demod_frame_id,
            observation_id=f.observation_id,
            timestamp_utc=f.timestamp_utc,
            field_name=f.field_name,
            field_value=f.field_value,
        )
        for f in fields
    ]
    session.add_all(rows)


# ---------------------------------------------------------------------------
# Convenience: fetch all demod frames for an observation as a Polars DF
# ---------------------------------------------------------------------------


def demod_frames_for_observation(
    session: Session, db_obs_id: int
) -> list[DemodFrameRow]:
    """Return all demod frames for one DB observation ID."""
    return session.query(DemodFrameRow).filter_by(observation_id=db_obs_id).all()
