"""Core configuration, constants, and Pydantic models for the entire project."""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from typing import Final

from pydantic import BaseModel, ConfigDict, Field, field_validator

# ─── Type Aliases ──────────────────────────────────────────────────────────────

# Simplified type aliases for SQL generators
type SyncSQLGen = Callable[[], str]
type AsyncSQLGen = Callable[[], Awaitable[str]]
type SQLGen = SyncSQLGen | AsyncSQLGen

# ─── Constants ──────────────────────────────────────────────────────────────────

DEFAULT_TIMEOUT_S: Final[int] = 30
MAX_PARALLEL_CHUNKS: Final[int] = 8
DEFAULT_CACHE_TTL: Final[int] = 60 * 60 * 24  # 24 hours
DEFAULT_MIN_GAMES: Final[int] = 10

# Patch information
PATCH_SELECT: Final[list[dict[str, str]]] = [
    {"name": "7.37", "date": "2024-08-01T07:30:27.355Z"},
    {"name": "7.38", "date": "2025-02-19T13:48:29.412Z"},
    {"name": "7.39", "date": "2025-05-22T23:36:01.602Z"},
]

LATEST_PATCH_TS: Final[str] = os.getenv("LATEST_PATCH_TS", PATCH_SELECT[-1]["date"])

# User agents for rotation
USER_AGENTS: Final[tuple[str, ...]] = (
    # Desktop Browsers
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    # Mobile Browsers
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.105 Mobile Safari/537.36",
)

# Endpoint-specific timeouts
TIMEOUTS: Final[dict[str, int]] = {
    "hero_grouping_stats": 60 * 6,
    "hero_pick_stats": 60 * 6,
    "hero_ban_stats": 60 * 6,
    "hero_recommendations": 60 * 5,
}

# Cache key prefixes
CACHE_PREFIXES: Final[dict[str, str]] = {
    "fetcher_status": "fetcher:status:",
    "fetcher_stats": "fetcher:stats:",
    "hero_synergy": "hero:synergy:",
    "hero_counter": "hero:counter:",
    "hero_recommend": "hero:recommend:",
}

# ─── Base Pydantic Models ───────────────────────────────────────────────────────


class BaseFetcherConfig(BaseModel):
    """Base Pydantic model for all fetcher configurations."""

    limit: int = Field(default=1000, ge=1, le=10000)
    max_parallel_chunks: int = Field(default=4, ge=1, le=MAX_PARALLEL_CHUNKS)
    skip_matches: bool = Field(default=False)
    force: bool = Field(default=False)

    model_config = ConfigDict(frozen=True, validate_assignment=True)

    @field_validator("max_parallel_chunks")
    @classmethod
    def validate_parallel_chunks(cls, v: int) -> int:
        """Ensure parallel chunks don't exceed CPU count."""
        cpu_count = os.cpu_count() or 4
        max_allowed = min(cpu_count * 2, MAX_PARALLEL_CHUNKS)
        if v > max_allowed:
            return max_allowed
        return v

    def check(self) -> None:
        """Performs additional validation if needed."""


class PassthroughModel(BaseModel):
    """A fallback Pydantic model that allows any extra fields."""

    model_config = ConfigDict(extra="allow", validate_assignment=True)
