# /home/ubuntu/dota/apps/players/models/player.py
# ================================================================================
"""The core Player model, representing a user's OpenDota profile."""

from __future__ import annotations

from django.db import models
from django.db.models import Q, QuerySet

__all__ = ("Player", "PlayerQuerySet")


class PlayerQuerySet(QuerySet["Player"]):
    """Custom QuerySet for the Player model with chainable filter methods."""

    def active(self) -> PlayerQuerySet:
        """Returns players who have a last match time recorded."""
        return self.exclude(last_match_time__isnull=True)

    def with_ranks(self):
        return self.select_related(
            "solo_rank",
            "competitive_rank",
            "rank_tier",
            "leaderboard_rank",
        )

    def with_steam_profile(self) -> PlayerQuerySet:
        """Returns players who have a complete public Steam profile."""
        return self.filter(~Q(steamid="") & ~Q(avatarfull="") & ~Q(profileurl=""))


class Player(models.Model):
    """Represents a player's profile and Steam metadata."""

    account_id = models.BigIntegerField(primary_key=True)
    steamid = models.CharField(max_length=32, blank=True, null=True)
    avatar = models.URLField(max_length=255, blank=True, null=True)
    avatarmedium = models.URLField(max_length=255, blank=True, null=True)
    avatarfull = models.URLField(max_length=255, blank=True, null=True)
    profileurl = models.URLField(max_length=255, blank=True, null=True)
    personaname = models.CharField(max_length=255, blank=True, null=True, db_index=True)
    plus = models.BooleanField(default=False, help_text="Whether the player has a Dota Plus subscription.")
    last_login = models.DateTimeField(blank=True, null=True)
    full_history_time = models.DateTimeField(blank=True, null=True, db_index=True)
    cheese = models.PositiveIntegerField(default=0, db_index=True, help_text="A legacy field from the OpenDota API.")
    fh_unavailable = models.BooleanField(
        blank=True,
        null=True,
        help_text="Indicates if full match history is unavailable.",
    )
    loccountrycode = models.CharField(max_length=2, blank=True, null=True)
    last_match_time = models.DateTimeField(blank=True, null=True, db_index=True)

    objects = PlayerQuerySet.as_manager()

    class Meta:
        db_table = "players"
        ordering = ["-last_match_time"]
        verbose_name = "Player"
        verbose_name_plural = "Players"
        indexes = [
            models.Index(fields=["personaname"], name="player_name_idx"),
            models.Index(fields=["loccountrycode"], name="player_country_idx"),
        ]

    def __str__(self) -> str:
        return self.personaname or f"Player {self.account_id}"
