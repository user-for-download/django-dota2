# /home/ubuntu/dota/apps/teams/models/scenario.py
# ================================================================================
"""
The TeamScenario model, for storing and analyzing win rates in specific,
recurring situations (e.g., Team A vs. Team B).
"""

from __future__ import annotations

import time
from typing import Self

from django.db import models
from django.db.models import Avg, Case, Count, DecimalField, F, GeneratedField, Max, Min, Q, Sum, Value, When

from apps.teams import conf


class TeamScenarioQuerySet(models.QuerySet["TeamScenario"]):
    """Custom QuerySet for the TeamScenario model with analytical helpers."""

    def by_region(self, region_id: int) -> Self:
        """Filters scenarios by a specific region ID."""
        return self.filter(region=region_id)

    def by_side(self, *, is_radiant: bool) -> Self:
        """Filters scenarios by the side played (Radiant or Dire)."""
        return self.filter(is_radiant=is_radiant)

    def recent_weeks(self, weeks: int = conf.DEFAULT_RECENT_WEEKS) -> Self:
        """Filters scenarios that occurred within the last N weeks."""
        # Calculate the cutoff week based on Unix epoch time.
        current_epoch_week = int(time.time()) // (7 * 24 * 3600)
        cutoff_week = current_epoch_week - weeks
        return self.filter(epoch_week__gte=cutoff_week)

    def high_sample_size(self, min_games: int = conf.DEFAULT_HIGH_SAMPLE_SIZE_GAMES) -> Self:
        """Filters for scenarios with a statistically significant number of games."""
        return self.filter(games__gte=min_games)

    async def aget_scenario_summary(self, team_id: int) -> dict:
        """
        Asynchronously aggregates a high-level performance summary for a team
        across all its recorded scenarios.
        """
        return (
            await self.filter(
                scenario__icontains=str(team_id),
            )
            .high_sample_size(5)
            .aaggregate(
                total_scenarios=Count("id"),
                avg_win_rate=Avg("win_rate"),
                total_games=Sum("games"),
                best_win_rate=Max("win_rate"),
                worst_win_rate=Min("win_rate"),
            )
        )


class TeamScenario(models.Model):
    """
    Represents an aggregated statistical scenario, such as head-to-head win rates.
    The `scenario` field is a flexible string, e.g., "TeamA_vs_TeamB".
    """

    # Scenario identifiers
    scenario = models.CharField(max_length=255, db_index=True, help_text="A unique string identifying the scenario.")
    is_radiant = models.BooleanField(
        null=True,
        blank=True,
        db_index=True,
        help_text="Side played in this scenario, if applicable.",
    )
    region = models.SmallIntegerField(null=True, blank=True, db_index=True)
    epoch_week = models.PositiveSmallIntegerField(null=True, blank=True, db_index=True)

    # Core stats
    games = models.PositiveBigIntegerField(default=1, db_index=True)
    wins = models.PositiveBigIntegerField(default=0, db_index=True)

    # Generated fields for performance and data consistency, calculated by the database.
    losses = GeneratedField(
        expression=F("games") - F("wins"),
        output_field=models.PositiveBigIntegerField(),
        db_persist=True,
    )
    win_rate = GeneratedField(
        expression=Case(
            When(games__gt=0, then=F("wins") * 100.0 / F("games")),
            default=Value(0.0),
        ),
        output_field=DecimalField(max_digits=5, decimal_places=2),
        db_persist=True,
    )

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    objects = TeamScenarioQuerySet.as_manager()

    class Meta:
        db_table = "team_scenarios"
        verbose_name = "Team Scenario"
        verbose_name_plural = "Team Scenarios"
        constraints = [
            # Ensure each scenario is unique per week, side, and region.
            models.UniqueConstraint(
                fields=("scenario", "is_radiant", "region", "epoch_week"),
                name="team_scenario_unique",
            ),
            # Database-level checks for data integrity.
            models.CheckConstraint(check=Q(games__gte=1), name="scn_games_positive"),
            models.CheckConstraint(check=Q(wins__lte=F("games")), name="scn_wins_not_exceed_games"),
        ]
        indexes = [
            models.Index(fields=["scenario", "region"]),
            models.Index(fields=["-win_rate"], name="ts_win_rate_idx"),
        ]

    def __str__(self) -> str:
        side = "Radiant" if self.is_radiant else "Dire"
        return f"{self.scenario} ({side}, Region {self.region}) - {self.win_rate:.1f}% WR"

    def to_dict(self) -> dict:
        """Serializes the model instance to a dictionary for API responses."""
        return {
            "scenario": self.scenario,
            "is_radiant": self.is_radiant,
            "region": self.region,
            "games": self.games,
            "wins": self.wins,
            "losses": self.losses,
            "win_rate": float(self.win_rate),
            "epoch_week": self.epoch_week,
            "updated_at": self.updated_at.isoformat(),
        }
