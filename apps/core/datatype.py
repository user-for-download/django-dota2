"""Core data types and type definitions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, NotRequired, Protocol, TypedDict, runtime_checkable

# Type aliases for better clarity
type JsonValue = str | int | float | bool | None
type JsonDict = dict[str, JsonValue]

# ─── Upsert Protocol & Types ─────────────────────────────


@runtime_checkable
class UpserTable(Protocol):
    """Protocol for objects representing upsert operation results."""

    created: int
    updated: int
    skipped: int


class UpsertResult(TypedDict):
    """Dictionary type for upsert operation results."""

    created: int
    updated: int
    skipped: int


# ─── Extended Result Types ──────────────────────


class CachePayload(UpsertResult):
    """Extended payload for cached data with metadata."""

    timestamp: NotRequired[str]  # ISO 8601 format
    processing_ms: NotRequired[float]
    cfg: NotRequired[JsonDict]


class FetcherResult(CachePayload):
    """Comprehensive result of a fetcher service operation."""

    source: NotRequired[str]  # 'cache', 'fresh', 'error'
    status: NotRequired[str]  # 'ok', 'validation_error', 'error'
    message: NotRequired[str]
    error: NotRequired[str]


class StatsPayload(TypedDict):
    """Aggregated statistics over multiple operations."""

    total_fetches: int
    proc_sum_ms: float
    created_sum: int
    updated_sum: int
    avg_ms: float


# ─── Configuration Objects ───────────────────────


@dataclass(slots=True, frozen=True, kw_only=True)
class CacheConfig:
    """Immutable cache configuration."""

    key: str
    timeout: int | None = None  # seconds, None = cache forever
    prefix: str = ""

    def __post_init__(self) -> None:
        """Validate cache key format."""
        if not self.key or not isinstance(self.key, str):
            msg = "Cache key must be a non-empty string"
            raise ValueError(msg)

    @property
    def full_key(self) -> str:
        """Get the full cache key with prefix."""
        return f"{self.prefix}{self.key}" if self.prefix else self.key


# ─── Factory Functions ──────────────────────


def new_upsert_result(
    *,
    created: int = 0,
    updated: int = 0,
    skipped: int = 0,
) -> UpsertResult:
    """Create a new UpsertResult with validation."""
    if any(v < 0 for v in (created, updated, skipped)):
        msg = "Upsert counts cannot be negative"
        raise ValueError(msg)
    return UpsertResult(created=created, updated=updated, skipped=skipped)


def as_upsert_result(obj: Any) -> UpsertResult:
    """Convert an object to UpsertResult format."""
    if isinstance(obj, dict):
        return new_upsert_result(
            created=int(obj.get("created", 0)),
            updated=int(obj.get("updated", 0)),
            skipped=int(obj.get("skipped", 0)),
        )

    # For objects with attributes
    return new_upsert_result(
        created=int(getattr(obj, "created", 0)),
        updated=int(getattr(obj, "updated", 0)),
        skipped=int(getattr(obj, "skipped", 0)),
    )


# ─── Result Aggregation ─────────────────────


@dataclass(slots=True)
class ResultAggregator:
    """Helper for aggregating multiple UpsertResults."""

    created: int = field(default=0)
    updated: int = field(default=0)
    skipped: int = field(default=0)

    def add(self, result: UpsertResult) -> None:
        """Add a result to the aggregation."""
        self.created += result.get("created", 0)
        self.updated += result.get("updated", 0)
        self.skipped += result.get("skipped", 0)

    def to_dict(self) -> UpsertResult:
        """Convert to UpsertResult dictionary."""
        return UpsertResult(
            created=self.created,
            updated=self.updated,
            skipped=self.skipped,
        )
