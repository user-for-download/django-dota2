# /home/ubuntu/dota/apps/teams/models/rating.py
# ================================================================================
"""The TeamRating model, storing performance metrics for a Team."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Self

from django.db import models
from django.utils import timezone

from apps.teams import conf


class TeamRatingQuerySet(models.QuerySet["TeamRating"]):
    """Custom QuerySet for the TeamRating model."""

    def with_team_info(self) -> Self:
        """Eager-loads the related `Team` object to prevent N+1 queries."""
        return self.select_related("team")

    def top_rated(self, limit: int = 100) -> Self:
        """Returns the highest-rated teams, ordered by rating and recent activity."""
        return self.with_team_info().filter(rating__isnull=False).order_by("-rating", "-last_match_time")[:limit]

    def active_teams(self, days: int = conf.DEFAULT_ACTIVE_TEAM_DAYS) -> Self:
        """Filters for ratings of teams that have played a match recently."""
        # Calculate the cutoff timestamp once.
        cutoff = timezone.now() - timedelta(days=days)
        return self.filter(last_match_time__gte=cutoff.timestamp())


class TeamRating(models.Model):
    """
    Stores rating and win/loss statistics for a single team.
    This is a one-to-one extension of the main `Team` model.
    """

    team = models.OneToOneField(
        "Team",
        on_delete=models.CASCADE,
        primary_key=True,
        db_column="team_id",
        related_name="rating",
        help_text="The team this rating belongs to.",
    )
    rating = models.PositiveIntegerField(
        null=True,
        blank=True,
        db_index=True,
        help_text="The team's rating, stored as an integer (e.g., 1234.56 is stored as 123456).",
    )
    wins = models.PositiveIntegerField(default=0, db_index=True)
    losses = models.PositiveIntegerField(default=0, db_index=True)

    last_match_time = models.BigIntegerField(
        null=True,
        blank=True,
        db_index=True,
        help_text="Epoch timestamp of the last match played.",
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    objects = TeamRatingQuerySet.as_manager()

    class Meta:
        db_table = "team_rating"
        verbose_name = "Team Rating"
        verbose_name_plural = "Team Ratings"

    def __str__(self) -> str:
        """Returns a human-readable representation of the team's rating."""
        rating_display = f"{self.rating_decimal:.2f}" if self.rating is not None else "N/A"
        return f"Team {self.team_id} - Rating: {rating_display}"

    @property
    def total_games(self) -> int:
        """Calculates the total number of games played."""
        return self.wins + self.losses

    @property
    def win_rate_percentage(self) -> float:
        """Calculates the win rate as a percentage."""
        if self.total_games == 0:
            return 0.0
        return (self.wins / self.total_games) * 100.0

    @property
    def rating_decimal(self) -> float | None:
        """Converts the stored integer rating back to a float for display."""
        return self.rating / 100.0 if self.rating is not None else None

    def to_dict(self) -> dict:
        """Serializes the model instance to a dictionary for API responses."""
        last_match_dt = datetime.fromtimestamp(self.last_match_time, tz=UTC) if self.last_match_time else None
        return {
            "team_id": self.team_id,
            "rating": self.rating_decimal,
            "wins": self.wins,
            "losses": self.losses,
            "total_games": self.total_games,
            "win_rate": round(self.win_rate_percentage, 2),
            "last_match_time": last_match_dt.isoformat() if last_match_dt else None,
            "updated_at": self.updated_at.isoformat(),
        }
