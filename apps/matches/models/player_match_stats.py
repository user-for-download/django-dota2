# /home/ubuntu/dota/apps/matches/models/player_match_stats.py
# ================================================================================
"""Side-table for heavyweight or rarely-queried per-player-per-match data."""

from __future__ import annotations

from django.db import models


class PlayerMatchStats(models.Model):
    """
    Stores less-frequently accessed stats for a player in a single match.
    This includes detailed damage numbers, support stats, and other parsed data.
    Separating this data from the main `PlayerMatch` table keeps the primary
    scoreboard table lean and fast.

    NOTE: Heavy time-series logs (e.g., purchase_log, gold_t) are intentionally
    omitted. They are better suited for a dedicated time-series database or a
    separate, specialized table if detailed querying is required.
    """

    player_match = models.OneToOneField(
        "matches.PlayerMatch",
        on_delete=models.CASCADE,
        primary_key=True,
        related_name="stats",
    )

    # Detailed stats from parsing
    hero_damage = models.PositiveIntegerField(null=True, blank=True)
    tower_damage = models.PositiveIntegerField(null=True, blank=True)
    hero_healing = models.PositiveIntegerField(null=True, blank=True)
    stuns = models.FloatField(null=True, blank=True, help_text="Total stun duration dealt in seconds.")

    # Vision & Support stats
    obs_placed = models.PositiveSmallIntegerField(null=True, blank=True)
    sen_placed = models.PositiveSmallIntegerField(null=True, blank=True)
    creeps_stacked = models.PositiveSmallIntegerField(null=True, blank=True)
    rune_pickups = models.PositiveSmallIntegerField(null=True, blank=True)

    # JSON blobs for complex but self-contained data structures
    ability_uses = models.JSONField(null=True, blank=True, help_text="e.g., {'ability_name': count}")
    damage_targets = models.JSONField(null=True, blank=True, help_text="e.g., {'hero_name': damage_amount}")

    class Meta:
        db_table = "player_match_stats"
        verbose_name = "Player Match Stats"
        verbose_name_plural = "Player Match Stats"

    def __str__(self) -> str:
        return f"Detailed stats for {self.player_match}"
