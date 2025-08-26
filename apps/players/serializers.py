# /home/ubuntu/dota/apps/players/serializers.py
# ================================================================================
"""
High-performance, read-only serializers for Player and related models.
This approach avoids reflection overhead for faster API responses.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .models import Player, PlayerMatchHistory, PlayerRating, RankTier


class PlayerSerializer:
    """A collection of static methods for serializing player-related models."""

    @staticmethod
    def serialize_list_items(players_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Serializes player list items with proper datetime formatting."""
        for item in players_data:
            if isinstance(item.get("last_match_time"), datetime):
                item["last_match_time"] = item["last_match_time"].isoformat()
        return players_data

    @staticmethod
    def serialize_pro_list_item(player: Player) -> dict:
        """Serializes a player for the professional player list view."""
        profile = player.notable_profile
        return (
            {
                "account_id": player.account_id,
                "name": profile.name,
                "country_code": profile.country_code,
                "team_id": profile.team_id,
                "team_name": profile.team_name,
                "team_tag": profile.team_tag,
            }
            if profile
            else {"account_id": player.account_id, "name": player.personaname}
        )

    @staticmethod
    def serialize_player_detail(player: Player, *, include_notable_profile: bool, include_ranks: bool) -> dict:
        """Serializes a single Player instance with optional related data."""
        data = {
            "account_id": player.account_id,
            "steamid": player.steamid,
            "avatarfull": player.avatarfull,
            "profileurl": player.profileurl,
            "personaname": player.personaname,
            "plus": player.plus,
            "last_login": player.last_login.isoformat() if player.last_login else None,
            "loccountrycode": player.loccountrycode,
            "last_match_time": player.last_match_time.isoformat() if player.last_match_time else None,
        }
        if include_notable_profile and hasattr(player, "notable_profile") and player.notable_profile:
            profile = player.notable_profile
            data["notable_profile"] = {
                "name": profile.name,
                "country_code": profile.country_code,
                "is_pro": profile.is_pro,
                "team_id": profile.team_id,
                "team_name": profile.team_name,
                "team_tag": profile.team_tag,
            }
        if include_ranks:
            data["ranks"] = {
                "solo_mmr": getattr(player.solo_rank, "rating", None) if hasattr(player, "solo_rank") else None,
                "party_mmr": getattr(player.competitive_rank, "rating", None) if hasattr(player, "competitive_rank") else None,
                "rank_tier_int": getattr(player.rank_tier, "rating", None) if hasattr(player, "rank_tier") else None,
                "rank_tier_str": getattr(player.rank_tier, "medal", None) if hasattr(player, "rank_tier") else None,
                "leaderboard_rank": getattr(player.leaderboard_rank, "rating", None) if hasattr(player, "leaderboard_rank") else None,
            }
        return data

    @staticmethod
    def serialize_match_history(entry: PlayerMatchHistory) -> dict:
        """Serializes a PlayerMatchHistory entry."""
        return {"match_id": entry.match_id, "player_slot": entry.player_slot}

    @staticmethod
    def serialize_rating_history(rating: PlayerRating) -> dict:
        """Serializes a PlayerRating (historical MMR) entry."""
        return {
            "match_id": rating.match_id,
            "solo_mmr": rating.solo_competitive_rank,
            "party_mmr": rating.competitive_rank,
            "timestamp": rating.time.isoformat(),
        }

    @staticmethod
    def serialize_rank_tier(rank: RankTier) -> dict:
        """Serializes a RankTier object."""
        return {"account_id": rank.player_id, "rank": rank.rating, "medal": rank.medal}
