# /home/ubuntu/dota/apps/matches/models/player_match.py
# ================================================================================
"""
Core scoreboard statistics for a single player within a single match.
"""

from __future__ import annotations

from django.contrib.postgres.fields import ArrayField
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.db.models import BooleanField, Case, GeneratedField, Q, Value, When

from apps.matches.conf import MAX_PLAYER_LEVEL, MAX_PLAYER_SLOT, TEAM_SIZE


class PlayerMatch(models.Model):
    """
    Represents one player's performance in one match. This table contains the
    essential scoreboard data that is frequently queried.
    """

    id = models.BigAutoField(primary_key=True)

    # --- Foreign Keys ---
    match = models.ForeignKey(
        "matches.Match",
        on_delete=models.PROTECT,
        related_name="player_matches",
    )
    player = models.ForeignKey(
        "players.Player",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="match_history",
    )
    hero = models.ForeignKey("core.Hero", on_delete=models.PROTECT, related_name="appearances")

    # --- Slot & Side ---
    player_slot = models.PositiveSmallIntegerField(
        validators=[MinValueValidator(0), MaxValueValidator(MAX_PLAYER_SLOT)],
        db_index=True,
    )
    is_radiant = GeneratedField(
        expression=Case(
            When(player_slot__lt=TEAM_SIZE, then=Value(True)),
            default=Value(False),
        ),
        output_field=BooleanField(),
        db_persist=True,
    )

    # --- Scoreboard Numbers ---
    kills = models.PositiveSmallIntegerField(default=0)
    deaths = models.PositiveSmallIntegerField(default=0)
    assists = models.PositiveSmallIntegerField(default=0)
    last_hits = models.PositiveSmallIntegerField(default=0)
    denies = models.PositiveSmallIntegerField(default=0)
    gold_per_min = models.PositiveSmallIntegerField(default=0)
    xp_per_min = models.PositiveSmallIntegerField(default=0)
    level = models.PositiveSmallIntegerField(
        default=1,
        validators=[MinValueValidator(1), MaxValueValidator(MAX_PLAYER_LEVEL)],
    )
    net_worth = models.PositiveIntegerField(default=0)

    # --- Itemization ---
    items = ArrayField(models.PositiveIntegerField(), size=6, default=list, blank=True)
    backpack = ArrayField(models.PositiveIntegerField(), size=3, default=list, blank=True)
    item_neutral = models.PositiveIntegerField(null=True, blank=True)

    @property
    def kda_ratio(self) -> float:
        """Calculates the KDA ratio. Handles the case of zero deaths."""
        # REFACTOR: Correctly handle case where kills+assists is 0.
        if self.deaths == 0:
            return float(self.kills + self.assists)
        return round((self.kills + self.assists) / self.deaths, 2)

    class Meta:
        db_table = "player_matches"
        ordering = ["match", "player_slot"]
        unique_together = [("match", "player_slot")]
        indexes = [
            models.Index(fields=["player", "hero"]),
            models.Index(fields=["match", "hero"]),
        ]
        constraints = [
            models.CheckConstraint(
                name="player_slot_valid_range",
                check=Q(player_slot__gte=0, player_slot__lte=MAX_PLAYER_SLOT),
            ),
        ]

    def __str__(self) -> str:
        pname = getattr(self.player, "personaname", f"Anon Player {self.player_id}")
        hname = getattr(self.hero, "localized_name", f"Hero {self.hero_id}")
        return f"{pname} on {hname} (Match {self.match_id})"
