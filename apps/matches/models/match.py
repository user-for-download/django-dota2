# /home/ubuntu/dota/apps/matches/models/match.py
# ================================================================================
"""The core Match model – designed to be lean for high-traffic queries."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Self

from django.db import models
from django.db.models import Q
from django.utils import timezone

from apps.matches.conf import Winner


class MatchQuerySet(models.QuerySet["Match"]):
    """Custom queryset for the Match model with chainable filter methods."""

    def finished(self) -> Self:
        """Returns only matches that have a definitive winner."""
        return self.filter(winner__in=[Winner.RADIANT, Winner.DIRE])

    def recent(self, *, days: int = 7) -> Self:
        """Returns matches that started in the last N days."""
        cutoff = timezone.now() - timedelta(days=days)
        return self.filter(start_time__gte=int(cutoff.timestamp()))

    def with_stats(self) -> Self:
        """Eager-loads the related `MatchStats` object to prevent N+1 queries."""
        return self.select_related("stats")

    def by_patch(self, major: int, minor: int) -> Self:
        return self.filter(patch_info__patch_major=major, patch_info__patch_minor=minor)

    def by_date_range(self, start: datetime, end: datetime) -> Self:
        return self.filter(start_time__gte=int(start.timestamp()), start_time__lte=int(end.timestamp()))


class MatchManager(models.Manager.from_queryset(MatchQuerySet)):
    """Exposes the MatchQuerySet methods on the default manager (Match.objects)."""


class Match(models.Model):
    """
    A lean representation of a single Dota 2 match, containing only the most
    frequently queried data.
    """

    match_id = models.BigIntegerField(primary_key=True)
    match_seq_num = models.PositiveBigIntegerField(
        null=True,
        blank=True,
        db_index=True,
        db_comment="A sequence number for tracking new matches from the API.",
    )
    start_time = models.PositiveIntegerField(
        db_index=True,
        db_comment="Epoch timestamp of when the match began.",
    )
    duration = models.PositiveIntegerField(
        help_text="Match duration in seconds.",
    )
    winner = models.PositiveSmallIntegerField(
        choices=Winner.choices,
        default=Winner.UNKNOWN,
        db_index=True,
        db_comment="0=Dire, 1=Radiant, 2=Unknown.",
    )
    radiant_score = models.PositiveSmallIntegerField(default=0)
    dire_score = models.PositiveSmallIntegerField(default=0)

    # --- Foreign Keys ---
    league = models.ForeignKey(
        "leagues.League",
        on_delete=models.SET_NULL,
        related_name="matches",
        to_field="league_id",
        db_column="leagueid",
        null=True,
        blank=True,
        db_comment="FK to the leagues.league table.",
    )
    radiant_team = models.ForeignKey(
        "teams.Team",
        on_delete=models.SET_NULL,
        related_name="radiant_matches",  # Unique related_name is crucial.
        to_field="team_id",
        db_column="radiant_team_id",
        null=True,
        blank=True,
        db_comment="FK to the teams.team table for the radiant side.",
    )
    dire_team = models.ForeignKey(
        "teams.Team",
        on_delete=models.SET_NULL,
        related_name="dire_matches",  # Unique related_name is crucial.
        to_field="team_id",
        db_column="dire_team_id",
        null=True,
        blank=True,
        db_comment="FK to the teams.team table for the dire side.",
    )

    # --- Timestamps ---
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    objects = MatchManager()

    class Meta:
        db_table = "matches"
        ordering = ["-start_time"]
        verbose_name = "Match"
        verbose_name_plural = "Matches"
        constraints = [
            models.CheckConstraint(
                name="duration_non_negative",
                check=Q(duration__gte=0),
            ),
        ]

    def __str__(self) -> str:
        return f"Match {self.match_id}"
