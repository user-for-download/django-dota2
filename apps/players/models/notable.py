# /home/ubuntu/dota/apps/players/models/notable.py
# ================================================================================
"""The NotablePlayer model, for professional or otherwise notable player metadata."""

from __future__ import annotations

from django.db import models
from django.db.models import Q, QuerySet

__all__ = ("NotablePlayer",)


class NotableQuerySet(QuerySet["NotablePlayer"]):
    """Custom QuerySet for the NotablePlayer model."""

    def pro(self) -> NotableQuerySet:
        """Returns only players marked as professional."""
        return self.filter(is_pro=True)


class NotablePlayer(models.Model):
    """
    A one-to-one extension of the Player model, storing data specific to
    professional or other notable players.
    """

    player = models.OneToOneField(
        "Player",
        on_delete=models.CASCADE,
        primary_key=True,
        related_name="notable_profile",
        db_column="account_id",
    )
    name = models.CharField(max_length=255, blank=True, null=True, help_text="The player's professional name/handle.")
    country_code = models.CharField(max_length=2, blank=True, null=True)
    fantasy_role = models.SmallIntegerField(blank=True, null=True)
    team = models.ForeignKey(
        "teams.Team",
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="notable_players",
        db_index=True,
    )
    is_locked = models.BooleanField(
        default=False,
        help_text="Indicates if the player's team is locked for a tournament.",
    )
    is_pro = models.BooleanField(default=False, db_index=True)
    locked_until = models.DateTimeField(blank=True, null=True)
    is_current_team_member = models.BooleanField(default=False, db_index=True)

    objects = NotableQuerySet.as_manager()

    class Meta:
        db_table = "notable_players"
        ordering = ["name"]
        verbose_name = "Notable Player"
        verbose_name_plural = "Notable Players"
        indexes = [
            models.Index(fields=["is_pro", "is_locked"], name="player_pro_idx"),
            models.Index(fields=["fantasy_role"], name="player_role_idx"),
        ]
        constraints = [
            models.CheckConstraint(
                check=Q(fantasy_role__gte=0) | Q(fantasy_role__isnull=True),
                name="fantasy_role_non_negative",
            ),
        ]

    def __str__(self) -> str:
        return self.name or f"Notable Player {self.player_id}"

    @property
    def player_id(self) -> int:
        """A convenience property to access the related player's ID."""
        return self.player.account_id

    @property
    def team_name(self) -> str | None:
        """A convenience property to safely access the player's team name."""
        return self.team.name if self.team else None

    @property
    def team_tag(self) -> str | None:
        """A convenience property to safely access the player's team tag."""
        return self.team.tag if self.team else None
