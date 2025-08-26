# /home/ubuntu/dota/apps/teams/models/team.py
# ================================================================================
"""The core Team model and its associated manager/queryset."""

from __future__ import annotations

from typing import Any, Self

from django.db import models
from django.db.models import Count, Prefetch, Q
from django.urls import reverse
from django.utils.translation import gettext_lazy as _


class TeamQuerySet(models.QuerySet["Team"]):
    """Custom QuerySet for the Team model with performance optimizations."""

    def with_rating(self) -> Self:
        """Eager-loads the related `TeamRating` object to prevent N+1 queries."""
        return self.select_related("rating")

    def with_player_count(self) -> Self:
        """Annotates each team with the count of its current members."""
        return self.annotate(
            player_count=Count(
                "notable_players",
                filter=Q(notable_players__is_current_team_member=True),
                distinct=True,
            ),
        )

    def with_complete_data(self) -> Self:
        """A convenience method to fetch a team with all common related data."""
        from apps.players.models import NotablePlayer

        return self.select_related("rating").prefetch_related(
            Prefetch(
                "notable_players",
                queryset=NotablePlayer.objects.filter(is_current_team_member=True),
                to_attr="current_players",
            ),
        )

    def leaderboard(self, *, min_rating_int: int = 0, limit: int = 100) -> Self:
        return (
                   self.with_rating()
                   .with_player_count()
                   .filter(
                       is_active=True,
                       rating__rating__isnull=False,
                       rating__rating__gte=min_rating_int,
                       rating__wins__gt=0,
                   )
                   .order_by("-rating__rating", "-rating__last_match_time")  # ✅ DB fields only
               )[:limit]

    def search_by_name(self, query: str) -> Self:
        """Filters teams by name or tag, case-insensitively."""
        return self.filter(Q(name__icontains=query) | Q(tag__icontains=query)).distinct()


class Team(models.Model):
    """Represents a professional or amateur Dota 2 team."""

    team_id = models.BigIntegerField(primary_key=True)
    name = models.CharField(max_length=255, blank=True, db_index=True)
    tag = models.CharField(max_length=64, blank=True, db_index=True)
    logo_url = models.URLField(max_length=500, blank=True)
    is_active = models.BooleanField(default=True, db_index=True, help_text="Whether the team is considered active.")

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    objects = TeamQuerySet.as_manager()

    class Meta:
        db_table = "teams"
        ordering = ["-team_id"]
        verbose_name = _("team")
        verbose_name_plural = _("teams")
        indexes = [
            models.Index(fields=["name"]),
            models.Index(fields=["tag"]),
            models.Index(fields=["is_active", "-team_id"]),
        ]

    def __str__(self) -> str:
        return self.display_name

    def get_absolute_url(self) -> str:
        """Returns the canonical URL for a team's detail view."""
        return reverse("teams:detail", args=[self.team_id])

    @property
    def display_name(self) -> str:
        """Returns the team's name, or a fallback if the name is not set."""
        return self.name or f"Team {self.team_id}"

    def to_dict(self, *, include_rating: bool = False, include_players: bool = False) -> dict[str, Any]:
        """
        Serializes the model instance to a dictionary for API responses.
        This pattern is often faster than reflection-based serializers for read-heavy APIs.

        Args:
            include_rating: If True, include the nested rating dictionary.
            include_players: If True, include the `player_count` annotation.
        """
        data = {
            "team_id": self.team_id,
            "name": self.name,
            "tag": self.tag,
            "logo_url": self.logo_url,
            "is_active": self.is_active,
            "updated_at": self.updated_at.isoformat(),
        }
        if include_rating and hasattr(self, "rating") and self.rating:
            data["rating"] = self.rating.to_dict()
        if include_players and hasattr(self, "player_count"):
            data["player_count"] = self.player_count
        return data
