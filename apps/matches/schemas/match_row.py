# /home/ubuntu/dota/apps/matches/schemas/match_row.py
# ================================================================================
"""
Defines the MatchRow dataclass, a schema for validating and structuring raw
match data from an external source before it is processed into Django models.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from typing import Any

from apps.matches.conf import Winner


@dataclass(frozen=True, slots=True)
class MatchRow:
    """
    A Data Transfer Object (DTO) representing a single, comprehensive match record.

    This class acts as a validation layer and a structured container for data
    fetched from an external API. It cleanly separates concerns, allowing the
    data handler to work with a predictable object instead of a raw dictionary.
    """

    # Core Match fields
    match_id: int
    match_seq_num: int | None = None
    start_time: int | None = None
    duration: int | None = None
    winner: int | None = None  # Uses Winner enum values
    radiant_score: int | None = None
    dire_score: int | None = None
    league_id: int | None = None
    radiant_team_id: int | None = None
    dire_team_id: int | None = None

    # MatchStats fields (side-table)
    tower_status_radiant: int | None = None
    tower_status_dire: int | None = None
    barracks_status_radiant: int | None = None
    barracks_status_dire: int | None = None
    first_blood_time: int | None = None
    game_mode: int | None = None
    lobby_type: int | None = None
    human_players: int | None = None
    radiant_gold_adv: list[int] | None = None
    radiant_xp_adv: list[int] | None = None
    chat: list[dict[str, Any]] | None = None
    objectives: list[dict[str, Any]] | None = None
    teamfights: list[dict[str, Any]] | None = None
    draft_timings: list[dict[str, Any]] | None = None

    # Child object lists
    picks_bans: list[dict[str, Any]] | None = None
    players: list[dict[str, Any]] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Returns a dictionary representation of the entire dataclass."""
        return asdict(self)

    def match_fields(self) -> dict[str, Any]:
        """
        Returns a dictionary containing only the fields for the `Match` model.
        This is a crucial helper for the data handler to perform targeted upserts.
        """
        # Fields to exclude (i.e., those that belong to related models).
        exclude = {
            "picks_bans",
            "players",
            "match_id",
            "tower_status_radiant",
            "tower_status_dire",
            "barracks_status_radiant",
            "barracks_status_dire",
            "first_blood_time",
            "game_mode",
            "lobby_type",
            "human_players",
            "radiant_gold_adv",
            "radiant_xp_adv",
            "chat",
            "objectives",
            "teamfights",
            "draft_timings",
        }
        return {
            f.name: getattr(self, f.name)
            for f in fields(self)
            if f.name not in exclude and getattr(self, f.name) is not None
        }

    def stats_fields(self) -> dict[str, Any]:
        """Returns a dictionary containing only the fields for the `MatchStats` model."""
        include = {
            "tower_status_radiant",
            "tower_status_dire",
            "barracks_status_radiant",
            "barracks_status_dire",
            "first_blood_time",
            "game_mode",
            "lobby_type",
            "human_players",
            "radiant_gold_adv",
            "radiant_xp_adv",
            "chat",
            "objectives",
            "teamfights",
            "draft_timings",
        }
        return {
            f.name: getattr(self, f.name)
            for f in fields(self)
            if f.name in include and getattr(self, f.name) is not None
        }

    @staticmethod
    def parse(src: dict[str, Any]) -> MatchRow | None:
        """
        A factory method to safely parse a raw dictionary into a MatchRow instance.
        Returns None if essential data (like match_id) is missing or invalid.
        """
        try:
            match_id = int(src["match_id"])
        except (KeyError, ValueError, TypeError):
            return None

        # Convert the boolean `radiant_win` to our integer-based `Winner` enum value.
        winner_val = Winner.UNKNOWN.value
        if (radiant_win := src.get("radiant_win")) is not None:
            winner_val = Winner.RADIANT.value if radiant_win else Winner.DIRE.value

        return MatchRow(
            match_id=match_id,
            match_seq_num=src.get("match_seq_num"),
            start_time=src.get("start_time"),
            duration=src.get("duration"),
            winner=winner_val,
            radiant_score=src.get("radiant_score"),
            dire_score=src.get("dire_score"),
            league_id=src.get("leagueid"),
            radiant_team_id=src.get("radiant_team_id"),
            dire_team_id=src.get("dire_team_id"),
            tower_status_radiant=src.get("tower_status_radiant"),
            tower_status_dire=src.get("tower_status_dire"),
            barracks_status_radiant=src.get("barracks_status_radiant"),
            barracks_status_dire=src.get("barracks_status_dire"),
            first_blood_time=src.get("first_blood_time"),
            game_mode=src.get("game_mode"),
            lobby_type=src.get("lobby_type"),
            human_players=src.get("human_players"),
            radiant_gold_adv=src.get("radiant_gold_adv"),
            radiant_xp_adv=src.get("radiant_xp_adv"),
            chat=src.get("chat"),
            objectives=src.get("objectives"),
            teamfights=src.get("teamfights"),
            draft_timings=src.get("draft_timings"),
            picks_bans=src.get("picks_bans"),
            players=src.get("players"),
        )
