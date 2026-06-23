"""Polars / Dataframely schemas for structured data in the pipeline."""

import dataframely as dy


class ObservationSchema(dy.Schema):
    """Schema for a DataFrame of SatNOGS observations."""

    observation_id = dy.Int64()
    origin = dy.String()
    satellite_norad = dy.Int64()
    start_utc = dy.Datetime()
    end_utc = dy.Datetime()
    transmitter = dy.String()
    status = dy.String()
    vetted_status = dy.String()
    has_audio = dy.Bool()


class DemodFrameSchema(dy.Schema):
    """Schema for a DataFrame of demodulated frames."""

    observation_id = dy.Int64()
    origin = dy.String()
    algorithm = dy.String()
    timestamp_utc = dy.Datetime()
    hex_data = dy.String()


class DecodedFieldSchema(dy.Schema):
    """Schema for a DataFrame of decoded telemetry fields."""

    demod_frame_id = dy.Int64()
    observation_id = dy.Int64()
    origin = dy.String()
    timestamp_utc = dy.Datetime()
    field_name = dy.String()
    field_value = dy.String()
