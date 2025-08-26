# /home/ubuntu/dota/apps/players/models/history.py
# ================================================================================
"""The PlayerMatchHistory model for tracking processed matches."""
from __future__ import annotations

from django.db import models

__all__ = ("PlayerMatchHistory",)


class PlayerMatchHistory(models.Model):
    """
    Tracks which matches have already been fetched and processed for a player,
    preventing duplicate work in a data ingestion pipeline.
    """
    player = models.ForeignKey(
        "Player",
        on_delete=models.CASCADE,
        related_name="match_history_entries",
        db_column="account_id",
    )
    match_id = models.BigIntegerField()
    player_slot = models.SmallIntegerField(blank=True, null=True)
    retries = models.PositiveSmallIntegerField(
        default=0, db_index=True, help_text="Counter for processing retries, if any.",
    )

    class Meta:
        db_table = "player_match_history"
        unique_together = [["match_id", "player"]]
        verbose_name = "Player Match History"
        verbose_name_plural = "Player Match Histories"
        indexes = [
            models.Index(fields=["match_id"], name="pmh_match_idx"),
        ]

    def __str__(self) -> str:
        return f"Player {self.player_id} - Match {self.match_id} processed"
