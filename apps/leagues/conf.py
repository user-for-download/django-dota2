# /home/ubuntu/dota/apps/leagues/conf.py
# ================================================================================
"""Configuration, constants, and Pydantic models for the 'leagues' app."""

from __future__ import annotations

from typing import Final

from pydantic import BaseModel, ConfigDict

from apps.core.conf import BaseFetcherConfig

# ─── Service & Handler Defaults ───────────────────────────────────────────────
LEAGUES_FALLBACK_URL: Final[str] = "https://api.opendota.com/api/leagues"

# ─── Cache Timeouts ────────────────────────────────────────────────────────────
# Timeouts in seconds for league-related API endpoints.
LEAGUE_TIMEOUTS: Final[dict[str, int]] = {
    "list": 60 * 10,  # League list changes infrequently.
    "detail": 60 * 30,  # League details are very stable.
}

# ─── Pydantic Configuration & Validation Models ────────────────────────────────


class LeagueFetcherConfig(BaseFetcherConfig):
    """Configuration for the LeagueFetcher. Inherits base settings."""

    model_config = ConfigDict(frozen=True)

    def __str__(self) -> str:
        return f"LeagueFetcherConfig(limit={self.limit}, force={self.force}, skip_matches={self.skip_matches})"

class LeagueValidator(BaseModel):
    """
    Validates a single raw data row for 'leagues'. This acts as a data contract,
    ensuring that incoming data has the required fields and types.
    """

    leagueid: int
    ticket: str | None = None
    banner: str | None = None
    tier: str | None = None
    name: str | None = None  # Added name for completeness

    class Config:
        extra = "allow"  # Allow other fields from the API source.
        frozen = True  # Make the validated model immutable.
