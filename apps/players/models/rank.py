# /home/ubuntu/dota/apps/players/models/rank.py
# ================================================================================
"""
Models for storing player rank information, including modern medal tiers
and legacy MMR values.
"""
from __future__ import annotations

from django.db import models

__all__ = (
    "CompetitiveRank",
    "LeaderboardRank",
    "RankTier",
    "SoloCompetitiveRank",
)


class RankTier(models.Model):
    """
    Stores the modern Dota 2 rank tier (e.g., Divine 5), represented as an integer.
    """
    player = models.OneToOneField(
        "Player", on_delete=models.CASCADE, primary_key=True,
        related_name="rank_tier", db_column="account_id",
    )
    rating = models.PositiveIntegerField(
        blank=True, null=True, db_index=True,
        help_text="The integer representation of the rank tier (e.g., 75 for Divine 5).",
    )

    class Meta:
        db_table = "rank_tier"
        verbose_name = "Rank Tier"
        verbose_name_plural = "Rank Tiers"

    def __str__(self) -> str:
        return f"Rank Tier for {self.player_id}: {self.medal}"

    @property
    def medal(self) -> str:
        """
        Derives the human-readable medal name and star count from the integer rating.
        """
        if self.rating is None:
            return "Uncalibrated"

        medals = [
            "Uncalibrated", "Herald", "Guardian", "Crusader",
            "Archon", "Legend", "Ancient", "Divine", "Immortal",
        ]
        tier_index = self.rating // 10
        stars = self.rating % 10

        if not (1 <= tier_index < len(medals)):
            return "Unknown"

        medal_name = medals[tier_index]
        if medal_name == "Immortal":
            # Immortal rank has a leaderboard position, not stars.
            return medal_name
        return f"{medal_name} [{stars}]"


class LeaderboardRank(models.Model):
    """Stores a player's numerical rank on their regional leaderboard."""
    player = models.OneToOneField(
        "Player", on_delete=models.CASCADE, primary_key=True,
        related_name="leaderboard_rank", db_column="account_id",
    )
    rating = models.PositiveIntegerField(
        blank=True, null=True, help_text="The player's position on the leaderboard.",
    )

    class Meta:
        db_table = "leaderboard_rank"
        verbose_name = "Leaderboard Rank"
        verbose_name_plural = "Leaderboard Ranks"

    def __str__(self) -> str:
        return f"Leaderboard Rank for {self.player_id}: {self.rating}"


class SoloCompetitiveRank(models.Model):
    """Stores a player's legacy solo MMR value."""
    player = models.OneToOneField(
        "Player", on_delete=models.CASCADE, primary_key=True,
        related_name="solo_rank", db_column="account_id",
    )
    rating = models.PositiveIntegerField(blank=True, null=True)

    class Meta:
        db_table = "solo_competitive_rank"
        verbose_name = "Solo Competitive Rank"
        verbose_name_plural = "Solo Competitive Ranks"

    def __str__(self) -> str:
        return f"Solo Rank for {self.player_id}: {self.rating}"


class CompetitiveRank(models.Model):
    """Stores a player's legacy party MMR value."""
    player = models.OneToOneField(
        "Player", on_delete=models.CASCADE, primary_key=True,
        related_name="competitive_rank", db_column="account_id",
    )
    rating = models.PositiveIntegerField(blank=True, null=True)

    class Meta:
        db_table = "competitive_rank"
        verbose_name = "Competitive Rank"
        verbose_name_plural = "Competitive Ranks"

    def __str__(self) -> str:
        return f"Competitive Rank for {self.player_id}: {self.rating}"
