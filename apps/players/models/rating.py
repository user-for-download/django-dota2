"""
Historical MMR snapshots linked to matches.
"""
from __future__ import annotations

from django.db import models

__all__ = ("PlayerRating",)


class PlayerRating(models.Model):
    """Solo / party MMR over time."""

    player = models.ForeignKey(
        "Player", on_delete=models.CASCADE, related_name="ratings", db_column="account_id",
    )
    match_id = models.BigIntegerField(blank=True, null=True)
    solo_competitive_rank = models.PositiveIntegerField(blank=True, null=True)
    competitive_rank = models.PositiveIntegerField(blank=True, null=True)
    time = models.DateTimeField(db_index=True)

    class Meta:
        db_table = "player_ratings"
        unique_together = [["player", "time"]]
        ordering = ["-time"]
        verbose_name = "Player Rating"
        verbose_name_plural = "Player Ratings"
        indexes = [
            models.Index(fields=["match_id"], name="pr_match_idx"),
            models.Index(fields=["solo_competitive_rank"], name="pr_solo_idx"),
            models.Index(fields=["competitive_rank"], name="pr_party_idx"),
        ]

    def __str__(self) -> str:
        return f"Rating for {self.player_id} at {self.time}"
