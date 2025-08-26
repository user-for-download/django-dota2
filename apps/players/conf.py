# /home/ubuntu/dota/apps/players/conf.py
# ================================================================================
"""Configuration, constants, and Pydantic models for the 'players' app."""

from __future__ import annotations

from typing import Final

from pydantic import BaseModel, ConfigDict

from apps.core.conf import BaseFetcherConfig

# ─── Service & Handler Defaults ───────────────────────────────────────────────
PRO_PLAYERS_FALLBACK_URL: Final[str] = "https://api.opendota.com/api/proPlayers"

# ─── Management Command Defaults ──────────────────────────────────────────────
FETCH_PLAYERS_CMD_DEFAULT_LIMIT: Final[int] = 50
FETCH_PLAYERS_CMD_DEFAULT_MAX_PARALLEL: Final[int] = 5

# ─── Cache Timeouts ────────────────────────────────────────────────────────────
# Timeouts in seconds for various player-related API endpoints.
PLAYER_TIMEOUTS: Final[dict[str, int]] = {
    "list": 60 * 2,
    "pro_list": 60 * 10,
    "detail": 60 * 5,
    "matches": 60 * 5,
    "ratings": 60 * 60,
    "rank": 60 * 15,
    "stats": 60 * 60,
    "heroes": 60 * 60,
}

# ─── Pydantic Configuration & Validation Models ────────────────────────────────


class PlayerFetcherConfig(BaseFetcherConfig):
    """Configuration for the PlayerFetcher. Inherits base settings."""

    model_config = ConfigDict(frozen=True)


class PlayerValidator(BaseModel):
    """
    Validates a single raw data row for 'Players'. This acts as a data contract
    for incoming data from an external source.
    """

    account_id: int

    class Config:
        extra = "allow"  # Allow other fields from the API source.
        frozen = True  # Make the validated model immutable.
