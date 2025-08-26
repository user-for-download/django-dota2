# /home/ubuntu/dota/apps/matches/models/public_match.py
# ================================================================================
"""
A lightweight record of public (non-professional) matchmaking games.
"""

from __future__ import annotations

from django.contrib.postgres.fields import ArrayField
from django.core.validators import MinValueValidator
from django.db import models

from apps.matches.conf import TEAM_SIZE, GameMode, LobbyType


class PublicMatch(models.Model):
    """
    Stores summary data for public matchmaking games, distinct from professional
    or tournament matches stored in the main `Match` table.
    """

    match_id = models.BigIntegerField(primary_key=True)
    match_seq_num = models.PositiveBigIntegerField(null=True, blank=True, db_index=True)
    start_time = models.PositiveIntegerField(db_index=True, help_text="Epoch timestamp of match start.")
    duration = models.PositiveIntegerField(validators=[MinValueValidator(0)], help_text="Duration in seconds.")

    radiant_win = models.BooleanField()
    lobby_type = models.PositiveSmallIntegerField(choices=LobbyType.choices, db_index=True)
    game_mode = models.PositiveSmallIntegerField(choices=GameMode.choices, db_index=True)

    # Rank information for the match
    avg_rank_tier = models.PositiveSmallIntegerField(null=True, blank=True)
    num_rank_tier = models.PositiveSmallIntegerField(null=True, blank=True)

    # Hero compositions for each team
    radiant_team = ArrayField(models.PositiveIntegerField(), size=TEAM_SIZE)
    dire_team = ArrayField(models.PositiveIntegerField(), size=TEAM_SIZE)

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "public_matches"
        ordering = ["-start_time"]
        indexes = [
            # Index for common filtering on public match queues.
            models.Index(fields=["lobby_type", "game_mode", "start_time"]),
            # Index to support queries based on average rank.
            models.Index(fields=["avg_rank_tier"]),
        ]

    def __str__(self) -> str:
        return f"Public Match {self.match_id}"
