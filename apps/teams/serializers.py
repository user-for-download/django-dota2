# /home/ubuntu/dota/apps/teams/serializers.py
# ================================================================================
"""
High-performance, read-only serializers that use model methods.
This pattern is faster than reflection-based serializers like DRF for read-heavy APIs,
as it avoids introspection overhead. It centralizes the serialization logic for
different model types within the 'teams' app.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .models import Team, TeamMatch, TeamRating, TeamScenario


class TeamSerializer:
    """Serializes Team model instances."""

    @staticmethod
    def serialize_team(team: Team, *, include_rating: bool = False, include_players: bool = False) -> dict[str, Any]:
        """Serializes a single Team instance using its .to_dict() method."""
        return team.to_dict(include_rating=include_rating, include_players=include_players)

    @staticmethod
    def serialize_teams(
        teams: list[Team],
        *,
        include_rating: bool = False,
        include_players: bool = False,
    ) -> list[dict[str, Any]]:
        """Serializes a list of Team instances."""
        return [team.to_dict(include_rating=include_rating, include_players=include_players) for team in teams]

    @staticmethod
    def serialize_leaderboard(teams: list[Team]) -> dict[str, Any]:
        """Serializes data specifically for the leaderboard view, adding a 'rank'."""
        leaderboard_data = [
            {"rank": idx + 1, **team.to_dict(include_rating=True, include_players=True)}
            for idx, team in enumerate(teams)
        ]
        return {"count": len(leaderboard_data), "leaderboard": leaderboard_data}


class TeamRatingSerializer:
    """Serializes TeamRating model instances."""

    @staticmethod
    def serialize_rating(rating: TeamRating) -> dict[str, Any]:
        """Serializes a single TeamRating instance."""
        return rating.to_dict()

    @staticmethod
    def serialize_ratings(ratings: list[TeamRating]) -> list[dict[str, Any]]:
        """Serializes a list of TeamRating instances."""
        return [rating.to_dict() for rating in ratings]


class TeamMatchSerializer:
    """Serializes TeamMatch model instances."""

    @staticmethod
    def serialize_match(match: TeamMatch, *, include_result: bool = True) -> dict[str, Any]:
        """Serializes a single TeamMatch instance."""
        return match.to_dict(include_result=include_result)

    @staticmethod
    def serialize_matches(matches: list[TeamMatch], *, include_results: bool = True) -> list[dict[str, Any]]:
        """Serializes a list of TeamMatch instances."""
        return [match.to_dict(include_result=include_results) for match in matches]


class TeamScenarioSerializer:
    """Serializes TeamScenario model instances."""

    @staticmethod
    def serialize_scenario(scenario: TeamScenario) -> dict[str, Any]:
        """Serializes a single TeamScenario instance."""
        return scenario.to_dict()

    @staticmethod
    def serialize_scenarios(scenarios: list[TeamScenario]) -> list[dict[str, Any]]:
        """Serializes a list of TeamScenario instances."""
        return [scenario.to_dict() for scenario in scenarios]
