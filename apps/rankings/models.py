# /home/ubuntu/dota/apps/rankings/models.py
# ================================================================================
"""Database models for player rankings and statistical scenarios."""

from __future__ import annotations

from django.db import models


class HeroRanking(models.Model):
    """
    Stores player-specific rankings for each hero.

    This table holds a calculated 'score' for a player's performance on a
    specific hero, derived from their match history.
    """

    account_id = models.BigIntegerField(help_text="The player's 64-bit Steam account ID.")
    hero_id = models.IntegerField(help_text="The ID of the hero.")
    score = models.FloatField(
        null=True,
        blank=True,
        help_text="Calculated performance score for the player-hero pair.",
    )

    class Meta:
        db_table = "hero_ranking"
        verbose_name = "Hero Ranking"
        verbose_name_plural = "Hero Rankings"
        # Ensures each player has only one ranking entry per hero.
        unique_together = [["account_id", "hero_id"]]
        indexes = [
            # Index to accelerate queries that filter by hero and sort by score,
            # which is common for creating leaderboards.
            models.Index(fields=["hero_id", "score"], name="hero_ranking_hero_id_score_idx"),
        ]

    def __str__(self) -> str:
        """Returns a human-readable representation of the hero ranking."""
        return f"Player {self.account_id} - Hero {self.hero_id}: {self.score or 'N/A'}"


class Scenario(models.Model):
    """
    Aggregates win rates for various in-game scenarios.

    This model is flexible and can store different types of scenarios:
    - Item timings (e.g., win rate for a hero buying a specific item by a certain time).
    - Lane role performance (e.g., win rate for a hero in a specific lane at a certain time).
    """

    # Scenario identifiers
    hero_id = models.SmallIntegerField(null=True, blank=True, db_index=True)
    item = models.TextField(null=True, blank=True, help_text="Identifier for an item, if applicable.")
    time = models.IntegerField(null=True, blank=True, help_text="Time bucket in minutes (e.g., 15 for 15-min mark).")
    lane_role = models.SmallIntegerField(null=True, blank=True, help_text="Identifier for the lane role.")
    epoch_week = models.IntegerField(
        null=True,
        blank=True,
        db_index=True,
        help_text="The ISO week (YYYYWW) for the data.",
    )

    # Win/Loss statistics
    games = models.BigIntegerField(default=1)
    wins = models.BigIntegerField(null=True, blank=True)

    class Meta:
        db_table = "scenarios"
        verbose_name = "Scenario"
        verbose_name_plural = "Scenarios"
        constraints = [
            # Constraint for item-based scenarios.
            models.UniqueConstraint(
                fields=["hero_id", "item", "time", "epoch_week"],
                name="scenarios_hero_item_time_week_unique",
            ),
            # Constraint for lane-role-based scenarios.
            models.UniqueConstraint(
                fields=["hero_id", "lane_role", "time", "epoch_week"],
                name="scenarios_hero_lane_time_week_unique",
            ),
        ]

    def __str__(self) -> str:
        """Returns a human-readable summary of the scenario and its win rate."""
        if self.item:
            identifier = f"Hero {self.hero_id} - Item '{self.item}' @ {self.time} min"
        else:
            identifier = f"Hero {self.hero_id} - Lane {self.lane_role} @ {self.time} min"

        # REFACTOR: Prevent ZeroDivisionError and simplify the win rate calculation.
        win_rate = (self.wins / self.games * 100) if self.wins is not None and self.games > 0 else 0.0
        return f"{identifier} | {win_rate:.1f}% WR ({self.games} games)"
