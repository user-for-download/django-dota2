# /home/ubuntu/dota/apps/leagues/serializers.py
# ================================================================================
"""
High-performance serializers for the League model.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .models import League


class LeagueSerializer:
    """Serializes League model instances into dictionaries."""

    @staticmethod
    def serialize_league(league: League) -> dict[str, Any]:
        """
        Serializes a single League instance.
        Delegates the core logic to the model's `to_dict()` method for consistency.
        """
        return league.to_dict()

    @staticmethod
    def serialize_leagues(leagues: list[League]) -> list[dict[str, Any]]:
        """
        Serializes a list of League instances efficiently using a list comprehension.
        """
        return [league.to_dict() for league in leagues]
