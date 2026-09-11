"""create ingestion_manifest

Revision ID: 0001
Revises:
Create Date: 2026-09-11

"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "ingestion_manifest",
        sa.Column("url_hash", sa.Text(), nullable=False),
        sa.Column("normalised_url", sa.Text(), nullable=False),
        sa.Column("publisher", sa.Text(), nullable=False),
        sa.Column("url_type", sa.Text(), nullable=False),
        sa.Column("doi", sa.Text(), nullable=True),
        sa.Column("payload_hash", sa.Text(), nullable=True),
        # timezone=True on all three: PostgreSQL must store these as
        # "timestamp with time zone" or every refresh-window comparison is off
        # by the operator's UTC offset.
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_changed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "fetch_count",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("last_http_status", sa.Integer(), nullable=True),
        sa.Column("last_run_id", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("url_hash", name=op.f("pk_ingestion_manifest")),
    )
    op.create_index(
        "ix_ingestion_manifest_publisher_url_type",
        "ingestion_manifest",
        ["publisher", "url_type"],
        unique=False,
    )
    op.create_index(
        "ix_ingestion_manifest_publisher_status",
        "ingestion_manifest",
        ["publisher", "status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_ingestion_manifest_publisher_status",
        table_name="ingestion_manifest",
    )
    op.drop_index(
        "ix_ingestion_manifest_publisher_url_type",
        table_name="ingestion_manifest",
    )
    op.drop_table("ingestion_manifest")
