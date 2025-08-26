# /home/ubuntu/dota/common/messaging/types.py
# ================================================================================
"""
Defines shared data types and Pydantic models for the messaging system.
Using these types ensures consistency between message publishers and consumers.
"""

from __future__ import annotations

from typing import TypedDict

from pydantic import BaseModel, Field


# ─────────────────────────── telemetry return type ──────────────────────────
class PublishResult(TypedDict, total=False):
    queue: str
    initial_ids: int
    already_processed: int
    ids_to_publish: int
    ids_published: int
    batches_created: int
    batches_failed: int
    duration_s: float


class MatchBatchPayload(BaseModel):
    match_ids: list[int] = Field(..., description="IDs in this batch")
    force_update: bool = Field(False, description="force update even if processed")
    batch_id: str = Field(..., description="opID-seq e.g. a1b2c3d4-2")
    batch_number: int = Field(ge=1, description="1-based batch ordinal")
    total_batches: int = Field(ge=1, description="total batches in op")
    timestamp: float = Field(..., description="unix epoch")
    priority: int = Field(5, description="queue priority hint")
    concurrency: int = Field(10, description="suggested worker conc.")
