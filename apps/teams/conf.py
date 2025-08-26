# /home/ubuntu/dota/apps/teams/conf.py
# ================================================================================
"""Configuration, constants, and Pydantic validators for the 'teams' app."""

from __future__ import annotations

import os
from typing import Final

from django.core.cache import caches
from pydantic import BaseModel, Field, field_validator, model_validator

from apps.core.conf import BaseFetcherConfig

# ─── Cache & Timeout Constants ─────────────────────────────────────────────────
# Centralizing these values makes them easy to adjust and manage.

DEFAULT_CACHE_ALIAS: Final[str] = os.getenv("DEFAULT_CACHE_ALIAS", "default")
DEFAULT_CACHE = caches[DEFAULT_CACHE_ALIAS]

# Cache timeouts in seconds for various team-related API endpoints.
TIMEOUTS: Final[dict[str, int]] = {
    "team_list": 60 * 5,  # 5 minutes
    "team_detail": 60 * 15,  # 15 minutes
    "leaderboard": 60 * 30,  # 30 minutes
    "team_rating": 60 * 10,  # 10 minutes
    "team_matches": 60 * 5,  # 5 minutes
    "team_scenarios": 60 * 60,  # 1 hour
    "team_stats": 60 * 15,  # 15 minutes
}

# ─── API View Defaults ─────────────────────────────────────────────────────────
# Default values and limits for API endpoints to ensure consistent behavior.
DEFAULT_STATS_MIN_GAMES: Final[int] = 100
DEFAULT_LEADERBOARD_LIMIT: Final[int] = 100
MAX_LEADERBOARD_LIMIT: Final[int] = 500
DEFAULT_SCENARIO_MIN_GAMES: Final[int] = 5
DEFAULT_STATS_RECENT_MATCHES: Final[int] = 20
DEFAULT_STATS_TOP_SCENARIOS: Final[int] = 10
DEFAULT_ACTIVE_TEAM_DAYS: Final[int] = 180
DEFAULT_RECENT_WEEKS: Final[int] = 10
DEFAULT_HIGH_SAMPLE_SIZE_GAMES: Final[int] = 10

# ─── Service & Handler Defaults ───────────────────────────────────────────────
# Fallback URL for team data if the primary source is unavailable.
TEAMS_FALLBACK_URL: Final[str] = "https://api.opendota.com/api/teams"

# ─── Management Command Defaults ──────────────────────────────────────────────
# Default parameters for the `fetch_teams` management command.

FETCH_TEAMS_CMD_DEFAULT_LIMIT: Final[int] = 50
FETCH_TEAMS_CMD_DEFAULT_MIN_RATING: Final[int] = 0
FETCH_TEAMS_CMD_DEFAULT_MAX_PARALLEL: Final[int] = 5

# Default parameters for the `warm_team_caches` management command.
WARM_CACHE_CMD_LEADERBOARD_LIMIT: Final[int] = 100
WARM_CACHE_CMD_LIST_LIMIT: Final[int] = 50


# ─── Pydantic Configuration & Validation Models ────────────────────────────────


class TeamFetcherConfig(BaseFetcherConfig):
    """Configuration for TeamFetcher with enforced limits and validation."""

    min_rating: int = Field(default=1000, description="Minimum rating for teams to be included in fetches.")

    def check(self) -> None:
        """Run additional team-specific validations."""
        super().check()  # Run base class validations
        if self.min_rating < 0:
            msg = "min_rating must be a non-negative integer."
            raise ValueError(msg)


class TeamValidator(BaseModel):
    """
    Validates a single raw data row for 'teams'.
    This Pydantic model acts as a data contract for incoming team data, ensuring
    type correctness and basic consistency before processing.
    """

    team_id: int
    name: str | None = None
    tag: str | None = None
    logo_url: str | None = None
    rating: float | None = Field(default=None, ge=0)
    wins: int | None = Field(default=None, ge=0)
    losses: int | None = Field(default=None, ge=0)
    last_match_time: int | None = Field(default=None, ge=0)

    @field_validator("logo_url")
    @classmethod
    def _clean_url(cls, v: str | None) -> str | None:
        """Ensures logo_url is a valid HTTP/HTTPS URL or None."""
        if v and not v.startswith(("http://", "https://")):
            return None
        return v

    @model_validator(mode="after")
    def _validate_wins_losses(self) -> TeamValidator:
        """Ensures wins and losses are non-negative if both are present."""
        if self.wins is not None and self.losses is not None:
            if self.wins < 0 or self.losses < 0:
                msg = "wins and losses must be non-negative."
                raise ValueError(msg)
        return self

    class Config:
        extra = "allow"
        frozen = True
