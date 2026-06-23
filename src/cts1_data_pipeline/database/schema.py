"""SQLAlchemy ORM table definitions for the CTS-1 pipeline.

Designed for future extensibility:
- All tables carry an `origin` column identifying the data source.
- The schema is satellite-agnostic so non-SatNOGS sources can be added later.
"""

from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Shared declarative base for all ORM models."""


class ObservationRow(Base):
    """One satellite observation session (source: SatNOGS or future systems)."""

    __tablename__ = "observations"

    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True, autoincrement=True)

    # Source system identifier — e.g. "satnogs"
    origin: Mapped[str] = mapped_column(sa.String(64), nullable=False, index=True)

    # Source-system native ID (e.g. SatNOGS observation ID)
    external_id: Mapped[int] = mapped_column(sa.BigInteger, nullable=False, index=True)

    satellite_norad: Mapped[int] = mapped_column(sa.Integer, nullable=False, index=True)
    start_utc: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=False), nullable=False
    )
    end_utc: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=False), nullable=False
    )
    transmitter: Mapped[str] = mapped_column(sa.Text, nullable=False, default="")
    status: Mapped[str] = mapped_column(sa.String(64), nullable=False, default="")
    vetted_status: Mapped[str] = mapped_column(
        sa.String(64), nullable=False, default=""
    )

    # Audio content stored inline; no local file paths.
    audio_data: Mapped[bytes | None] = mapped_column(sa.LargeBinary, nullable=True)
    audio_content_type: Mapped[str | None] = mapped_column(
        sa.String(128), nullable=True
    )

    # TLE snapshot at time of observation
    tle_line1: Mapped[str | None] = mapped_column(sa.String(256), nullable=True)
    tle_line2: Mapped[str | None] = mapped_column(sa.String(256), nullable=True)

    __table_args__ = (
        sa.UniqueConstraint(
            "origin", "external_id", name="uq_observations_origin_external"
        ),
    )


class DemodFrameRow(Base):
    """A single hex frame produced by a demodulation run."""

    __tablename__ = "demod_frames"

    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True, autoincrement=True)

    # Source system
    origin: Mapped[str] = mapped_column(sa.String(64), nullable=False, index=True)

    # FK to observations.id
    observation_id: Mapped[int] = mapped_column(
        sa.Integer,
        sa.ForeignKey("observations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Algorithm / tool variant used
    algorithm: Mapped[str] = mapped_column(sa.String(128), nullable=False)

    # Timestamp extracted from gr_satellites output (best available)
    timestamp_utc: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=False), nullable=False, index=True
    )

    hex_data: Mapped[str] = mapped_column(sa.Text, nullable=False)


class DecodedFieldRow(Base):
    """One decoded telemetry field tied back to its raw demod frame."""

    __tablename__ = "decoded_fields"

    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True, autoincrement=True)

    # Source system
    origin: Mapped[str] = mapped_column(sa.String(64), nullable=False, index=True)

    # FK to demod_frames.id
    demod_frame_id: Mapped[int] = mapped_column(
        sa.Integer,
        sa.ForeignKey("demod_frames.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    observation_id: Mapped[int] = mapped_column(
        sa.Integer,
        sa.ForeignKey("observations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    timestamp_utc: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=False), nullable=False, index=True
    )

    field_name: Mapped[str] = mapped_column(sa.String(256), nullable=False)
    field_value: Mapped[str] = mapped_column(sa.Text, nullable=False)
