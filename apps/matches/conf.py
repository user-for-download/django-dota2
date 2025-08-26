# /home/ubuntu/dota/apps/matches/conf.py
# ================================================================================
"""Configuration, constants, and Pydantic models for the 'matches' app."""

from __future__ import annotations

from typing import Any, Final, Literal

from django.db import models
from pydantic import BaseModel, Field, model_validator

from apps.core.conf import BaseFetcherConfig

# ─── App-wide Constants ────────────────────────────────────────────────────────
# These constants define constraints and defaults used across the app's models and views.
TEAM_SIZE: Final[int] = 5
MAX_PLAYER_SLOT: Final[int] = 132
MAX_PLAYER_LEVEL: Final[int] = 30
MAX_DRAFT_ORDER: Final[int] = 23
MAX_LIMIT_MATCHES: Final[int] = 1000
MAX_DAYS_RANGE: Final[int] = 365

# ─── Cache Timeouts ────────────────────────────────────────────────────────────
# Timeouts in seconds for various match-related API endpoints.
TIMEOUTS: Final[dict[str, int]] = {
    "match_list": 60,
    "match_detail": 60 * 5,
    "match_players": 60 * 5,
    "longest_matches": 60 * 15,
    "match_comprehensive": 60 * 10,
}


# ─── Django Model Enums ────────────────────────────────────────────────────────
# Using IntegerChoices provides labels in the Django admin and is the standard way.


class Winner(models.IntegerChoices):
    """Represents the winning side of a match."""

    DIRE = 0, "Dire"
    RADIANT = 1, "Radiant"
    UNKNOWN = 2, "Unknown / In Progress"


class Side(models.IntegerChoices):
    """Represents the team side in a match or draft."""

    RADIANT = 0, "Radiant"
    DIRE = 1, "Dire"


class GameMode(models.IntegerChoices):
    """Represents the game mode, mapped from official Dota 2 API constants."""

    UNKNOWN = 0, "Unknown"
    ALL_PICK = 1, "All Pick"
    CAPTAINS_MODE = 2, "Captains Mode"
    RANDOM_DRAFT = 3, "Random Draft"
    SINGLE_DRAFT = 4, "Single Draft"
    ALL_RANDOM = 5, "All Random"
    INTRO = 6, "Intro"
    DIRETIDE = 7, "Diretide"
    REVERSE_CAPTAINS_MODE = 8, "Reverse Captains Mode"
    GREEVILING = 9, "The Greeviling"
    TUTORIAL = 10, "Tutorial"
    MID_ONLY = 11, "Mid Only"
    LEAST_PLAYED = 12, "Least Played"
    LIMITED_HERO_POOL = 13, "Limited Hero Pool"
    COMPENDIUM = 14, "Compendium"
    CUSTOM = 15, "Custom"
    CAPTAINS_DRAFT = 16, "Captains Draft"
    BALANCED_DRAFT = 17, "Balanced Draft"
    ABILITY_DRAFT = 18, "Ability Draft"
    EVENT = 19, "Event"
    ALL_RANDOM_DEATHMATCH = 20, "All Random Deathmatch"
    SOLO_MID_1V1 = 21, "1v1 Solo Mid"
    RANKED_ALL_PICK = 22, "Ranked All Pick"
    TURBO = 23, "Turbo"
    MUTATION = 24, "Mutation"
    COACHES_CHALLENGE = 25, "Coaches Challenge"


class LobbyType(models.IntegerChoices):
    """Represents the lobby type, mapped from official Dota 2 API constants."""

    INVALID = -1, "Invalid"
    UNRANKED = 0, "Unranked Matchmaking"
    PRACTICE = 1, "Practice"
    TOURNAMENT = 2, "Tournament"
    TUTORIAL = 3, "Tutorial"
    COOP_VS_BOTS = 4, "Co-op with Bots"
    TEAM_MATCH = 5, "Team Match"
    SOLO_QUEUE = 6, "Solo Queue"
    RANKED = 7, "Ranked Matchmaking"
    SOLO_MID_1V1 = 8, "1v1 Solo Mid"
    BATTLE_CUP = 9, "Battle Cup"
    LOCAL_BOTS = 10, "Local Bots Match"
    SPECTATOR = 11, "Spectator"
    EVENT = 12, "Event Game"
    GAUNTLET = 13, "Gauntlet"
    NEW_PLAYER_POOL = 14, "New Player Pool"
    FEATURED_HERO = 15, "Featured Hero"


# ─── Pydantic Fetcher Configuration ────────────────────────────────────────────


class MatchFetcherConfig(BaseFetcherConfig):
    """
    Configuration for MatchFetcher with enforced limits and validation.
    This provides a structured way to define filters for querying matches.
    """

    # Identifiers
    # league_ids: list[int] | None = Field(default=None)
    # team_ids: list[int] | None = Field(default=None)
    # player_account_ids: list[int] | None = Field(default=None)
    # hero_ids: list[int] | None = Field(default=None)
    match_ids: list[int] | None = Field(default=None)

    # Time Range (Now valid because `datetime` is imported)
    # start_date: str | None = Field(default=None)
    # end_date: str | None = Field(default=None)

    # Properties
    # min_duration: int | None = Field(default=None, ge=0)
    # is_parsed: bool | None = Field(default=None)

    skip_matches: Literal[True] = True

    @model_validator(mode="before")
    @classmethod
    def adjust_limit_based_on_match_ids(cls, data: Any) -> Any:
        """
        If match_ids are provided and a limit is not, set the limit to
        the number of match_ids. This runs *before* the model is created,
        allowing us to work with frozen instances.
        """
        # Ensure we are working with a dictionary
        if not isinstance(data, dict):
            return data  # Not a dict, pass through to other validators

        match_ids = data.get("match_ids")
        is_limit_set_by_user = "limit" in data

        if match_ids and not is_limit_set_by_user:
            # Modify the incoming data dictionary, not the model instance
            data["limit"] = len(match_ids)

        # You can also add the cross-field validation here
        if match_ids and is_limit_set_by_user:
            if data["limit"] > len(match_ids):
                msg = f"Limit ({data['limit']}) cannot exceed the number of match_ids provided ({len(match_ids)})."
                raise ValueError(
                    msg,
                )

        return data  # Always return the data for the next steps

    def check(self) -> None:
        super().check()


class MatchValidator(BaseModel):
    """
    Validates a single raw match data row.
    Used for ensuring type correctness prior to database ingestion.
    """

    match_id: int = Field(ge=1)
    start_time: int = Field(ge=0, description="Epoch timestamp (seconds)")
    duration: int = Field(ge=0, description="Duration in seconds")

    winner: int | None = Field(default=None, ge=0, le=2, description="0=Dire, 1=Radiant, 2=Unknown")

    radiant_score: int = Field(ge=0)
    dire_score: int = Field(ge=0)

    league_id: int | None = Field(default=None, ge=1, description="ID of the league if available")
    radiant_team_id: int | None = Field(default=None, ge=1)
    dire_team_id: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def _validate_winner_and_scores(self) -> MatchValidator:
        """
        Ensure score and winner match up logically:
        - If winner is Radiant (1), radiant score must be > dire score.
        - If winner is Dire (0), dire score must be > radiant score.
        """
        if self.winner == 1 and self.radiant_score <= self.dire_score:
            msg = "Radiant cannot be winner if radiant_score <= dire_score."
            raise ValueError(msg)
        if self.winner == 0 and self.dire_score <= self.radiant_score:
            msg = "Dire cannot be winner if dire_score <= radiant_score."
            raise ValueError(msg)
        return self

    class Config:
        extra = "allow"
        frozen = True
