"""Polars / Dataframely schemas for structured data in the pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

import dataframely as dy

if TYPE_CHECKING:
    import polars as pl


class ObservationSchema(dy.Schema):
    """Schema for a DataFrame of SatNOGS observations."""

    observation_id: Annotated[pl.Int64, dy.Column()]
    origin: Annotated[pl.String, dy.Column()]
    satellite_norad: Annotated[pl.Int64, dy.Column()]
    start_utc: Annotated[pl.Datetime, dy.Column()]
    end_utc: Annotated[pl.Datetime, dy.Column()]
    transmitter: Annotated[pl.String, dy.Column()]
    status: Annotated[pl.String, dy.Column()]
    vetted_status: Annotated[pl.String, dy.Column()]
    has_audio: Annotated[pl.Boolean, dy.Column()]


class DemodFrameSchema(dy.Schema):
    """Schema for a DataFrame of demodulated frames."""

    observation_id: Annotated[pl.Int64, dy.Column()]
    origin: Annotated[pl.String, dy.Column()]
    algorithm: Annotated[pl.String, dy.Column()]
    timestamp_utc: Annotated[pl.Datetime, dy.Column()]
    hex_data: Annotated[pl.String, dy.Column()]


class DecodedFieldSchema(dy.Schema):
    """Schema for a DataFrame of decoded telemetry fields."""

    demod_frame_id: Annotated[pl.Int64, dy.Column()]
    observation_id: Annotated[pl.Int64, dy.Column()]
    origin: Annotated[pl.String, dy.Column()]
    timestamp_utc: Annotated[pl.Datetime, dy.Column()]
    field_name: Annotated[pl.String, dy.Column()]
    field_value: Annotated[pl.String, dy.Column()]
