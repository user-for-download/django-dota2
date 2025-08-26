# /home/ubuntu/dota/apps/matches/models/match_stats.py
# ================================================================================
"""Side-table for heavyweight or less-frequently-queried per-match data."""

from __future__ import annotations

from django.contrib.postgres.fields import ArrayField
from django.db import models

from apps.matches.conf import GameMode, LobbyType


class MatchStats(models.Model):
    """
    Stores less-frequently accessed or large data objects for a match.
    This one-to-one side-table avoids bloating the main `matches` table,
    improving query performance for common use cases.
    """

    match = models.OneToOneField(
        "matches.Match",
        on_delete=models.CASCADE,
        primary_key=True,
        related_name="stats",
    )
    # Building Status
    tower_status_radiant = models.PositiveIntegerField()
    tower_status_dire = models.PositiveIntegerField()
    barracks_status_radiant = models.PositiveIntegerField()
    barracks_status_dire = models.PositiveIntegerField()

    # Match Info
    first_blood_time = models.PositiveIntegerField()
    game_mode = models.PositiveSmallIntegerField(choices=GameMode.choices)
    lobby_type = models.PositiveSmallIntegerField(choices=LobbyType.choices)
    human_players = models.PositiveSmallIntegerField(default=10)

    # Large JSONB Blobs (can be queried efficiently in PostgreSQL)
    radiant_gold_adv = ArrayField(models.IntegerField(), null=True, blank=True)
    radiant_xp_adv = ArrayField(models.IntegerField(), null=True, blank=True)
    objectives = models.JSONField(null=True, blank=True)
    chat = models.JSONField(null=True, blank=True, help_text="A list of chat events from the match.")
    teamfights = models.JSONField(null=True, blank=True)
    draft_timings = models.JSONField(null=True, blank=True)
    cosmetics = models.JSONField(null=True, blank=True)

    class Meta:
        db_table = "match_stats"
        verbose_name = "Match Stats"
        verbose_name_plural = "Match Stats"

    def __str__(self) -> str:
        return f"Stats for Match {self.match_id}"
