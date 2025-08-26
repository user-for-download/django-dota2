# /home/ubuntu/dota/apps/leagues/models/league.py
# ================================================================================
"""The League model, representing tournaments and competitions."""
from __future__ import annotations

from typing import Any, Self

from django.db import models
from django.db.models import Count, Exists, F, OuterRef, Value
from django.db.models.functions import Coalesce


class LeagueQuerySet(models.QuerySet["League"]):
    """Optimized queryset for the League model with chainable filters."""

    def professional(self) -> Self:
        """Filters for leagues considered professional tier."""
        return self.filter(tier__in=["premium", "professional"])

    def with_match_count(self) -> Self:
        """Annotates each league with its total number of associated matches."""
        return self.annotate(match_count=Count("matches"))

    def with_team_count(self) -> Self:
        return self.annotate(
            radiant_teams=Count("matches__radiant_team_id", distinct=True),
            dire_teams=Count("matches__dire_team_id", distinct=True),
            team_count=Coalesce(F("radiant_teams") + F("dire_teams"), Value(0)),
        )

    def active(self) -> Self:
        """
        OPTIMIZATION: Filters for leagues that have at least one associated match.
        Uses a more performant `Exists` subquery instead of a `JOIN` with `distinct()`.
        """
        from apps.matches.models import Match
        has_matches_subquery = Match.objects.filter(league_id=OuterRef("pk"))
        return self.annotate(has_matches=Exists(has_matches_subquery)).filter(has_matches=True)


class League(models.Model):
    """Represents a Dota 2 tournament or league's metadata."""
    league_id = models.BigIntegerField(primary_key=True)
    ticket = models.CharField(max_length=255, blank=True, default="")
    banner = models.URLField(max_length=255, blank=True, default="")
    tier = models.CharField(max_length=255, blank=True, default="", db_index=True)
    name = models.CharField(max_length=255, blank=True, default="", db_index=True)

    objects = LeagueQuerySet.as_manager()

    class Meta:
        db_table = "leagues"
        ordering = ["-league_id"]
        verbose_name = "League"
        verbose_name_plural = "Leagues"
        indexes = [
            models.Index(fields=["tier"], name="league_tier_idx"),
        ]

    def __str__(self) -> str:
        return self.name or f"League {self.league_id}"

    @property
    def is_professional(self) -> bool:
        """Returns True if the league is considered a professional-tier event."""
        return self.tier in {"premium", "professional"}

    def to_dict(self) -> dict[str, Any]:
        """
        Serializes the model instance to a dictionary for API responses.
        Includes any fields annotated by the queryset manager.
        """
        data = {
            "league_id": self.league_id,
            "name": self.name,
            "tier": self.tier,
            "banner_url": self.banner,
            "is_professional": self.is_professional,
        }
        if hasattr(self, "match_count"):
            data["match_count"] = self.match_count
        if hasattr(self, "team_count"):
            data["team_count"] = self.team_count
        return data
