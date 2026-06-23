"""Initial schema: observations, demod_frames, decoded_fields.

Revision ID: 0001_initial
"""

import sqlalchemy as sa
from alembic import op

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "observations",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("origin", sa.String(64), nullable=False),
        sa.Column("external_id", sa.BigInteger, nullable=False),
        sa.Column("satellite_norad", sa.Integer, nullable=False),
        sa.Column("start_utc", sa.DateTime(timezone=False), nullable=False),
        sa.Column("end_utc", sa.DateTime(timezone=False), nullable=False),
        sa.Column("transmitter", sa.Text, nullable=False, server_default=""),
        sa.Column("status", sa.String(64), nullable=False, server_default=""),
        sa.Column("vetted_status", sa.String(64), nullable=False, server_default=""),
        sa.Column("audio_data", sa.LargeBinary, nullable=True),
        sa.Column("audio_content_type", sa.String(128), nullable=True),
        sa.Column("tle_line1", sa.String(256), nullable=True),
        sa.Column("tle_line2", sa.String(256), nullable=True),
    )
    op.create_index("ix_observations_origin", "observations", ["origin"])
    op.create_index("ix_observations_external_id", "observations", ["external_id"])
    op.create_index(
        "ix_observations_satellite_norad", "observations", ["satellite_norad"]
    )
    op.create_unique_constraint(
        "uq_observations_origin_external", "observations", ["origin", "external_id"]
    )

    op.create_table(
        "demod_frames",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("origin", sa.String(64), nullable=False),
        sa.Column(
            "observation_id",
            sa.Integer,
            sa.ForeignKey("observations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("algorithm", sa.String(128), nullable=False),
        sa.Column("timestamp_utc", sa.DateTime(timezone=False), nullable=False),
        sa.Column("hex_data", sa.Text, nullable=False),
    )
    op.create_index("ix_demod_frames_origin", "demod_frames", ["origin"])
    op.create_index(
        "ix_demod_frames_observation_id", "demod_frames", ["observation_id"]
    )
    op.create_index("ix_demod_frames_timestamp_utc", "demod_frames", ["timestamp_utc"])

    op.create_table(
        "decoded_fields",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("origin", sa.String(64), nullable=False),
        sa.Column(
            "demod_frame_id",
            sa.Integer,
            sa.ForeignKey("demod_frames.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "observation_id",
            sa.Integer,
            sa.ForeignKey("observations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("timestamp_utc", sa.DateTime(timezone=False), nullable=False),
        sa.Column("field_name", sa.String(256), nullable=False),
        sa.Column("field_value", sa.Text, nullable=False),
    )
    op.create_index("ix_decoded_fields_origin", "decoded_fields", ["origin"])
    op.create_index(
        "ix_decoded_fields_demod_frame_id", "decoded_fields", ["demod_frame_id"]
    )
    op.create_index(
        "ix_decoded_fields_observation_id", "decoded_fields", ["observation_id"]
    )
    op.create_index(
        "ix_decoded_fields_timestamp_utc", "decoded_fields", ["timestamp_utc"]
    )


def downgrade() -> None:
    op.drop_table("decoded_fields")
    op.drop_table("demod_frames")
    op.drop_table("observations")
