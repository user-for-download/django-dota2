# /home/ubuntu/dota/apps/rankings/management/commands/refresh_hero_stats.py
# ================================================================================
"""
Django management command to rebuild the HeroRanking and Scenario tables
from the base PlayerMatch data.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from django.core.management.base import BaseCommand
from django.db import models, transaction
from django.db.models import Case, Count, F, IntegerField, Q, SmallIntegerField, Sum, Value
from django.db.models.expressions import When
from django.db.models.functions import Cast

from apps.matches.models import PlayerMatch
from apps.rankings.conf import LANE_ROLE_MAPPING, TIMING_CANDIDATE_COLS
from apps.rankings.models import HeroRanking, Scenario

if TYPE_CHECKING:
    from django.db.models import Expression


# ─── Helpers ───────────────────────────────────────────────────────────────────


def get_current_epoch_week() -> int:
    """
    Returns the current ISO week as a single integer (e.g., 202427).
    This format is useful for weekly data partitioning.
    """
    now = datetime.now(UTC)
    year, week, _ = now.isocalendar()
    return int(f"{year}{week:02}")


def calculate_hero_score(wins: int, games: int) -> float:
    """
    Calculates a simple performance score for a player on a hero.
    The score is based on win rate, weighted by the number of games played
    to give more significance to players with more experience.

    Args:
        wins: Total number of wins on the hero.
        games: Total number of games played on the hero.

    Returns:
        The calculated score as a float.
    """
    if games == 0:
        return 0.0
    win_rate = wins / games  # Value from 0.0 to 1.0
    log_games = math.log10(games + 1)  # Logarithmic scaling to reward more games
    return round(win_rate * log_games * 1000, 2)  # Scale for readability


def get_concrete_fields(model_cls: type[models.Model]) -> set[str]:
    """Returns a set of concrete field names for a given model."""
    return {f.name for f in model_cls._meta.get_fields() if f.concrete}


# ─── Data Refresh Logic ───────────────────────────────────────────────────────


def refresh_hero_rankings() -> None:
    """
    Rebuilds the entire `hero_ranking` table from `PlayerMatch` data.

    This function aggregates wins and games for each player-hero combination,
    calculates a score, and then bulk-replaces the existing ranking data.
    """

    # 1. Aggregate games and wins for each player-hero pair.
    player_hero_stats = (
        PlayerMatch.objects.values("player_id", "hero_id")
        .annotate(
            games_played=Count("id"),
            wins_calculated=Sum(
                Case(
                    When(is_radiant=True, match__winner=0, then=1),  # Radiant player wins
                    When(is_radiant=False, match__winner=1, then=1),  # Dire player wins
                    default=0,
                    output_field=IntegerField(),
                ),
            ),
        )
        .order_by("player_id", "hero_id")
    )

    # 2. Calculate scores and prepare HeroRanking objects in memory.
    batch: list[HeroRanking] = []
    for row in player_hero_stats.iterator():
        score = calculate_hero_score(row["wins_calculated"], row["games_played"])
        batch.append(
            HeroRanking(
                account_id=row["player_id"],
                hero_id=row["hero_id"],
                score=score,
            ),
        )

    # 3. Truncate and bulk-insert the new rankings in a single transaction.
    # This approach is simple and effective for tables that can afford brief
    # read inconsistencies during the update window.
    with transaction.atomic():
        HeroRanking.objects.all().delete()
        HeroRanking.objects.bulk_create(batch, batch_size=2000)


def _get_time_bucket_expression() -> Expression:
    """
    Dynamically creates a Django ORM expression to calculate a time bucket
    in minutes. It searches for predefined timing columns in PlayerMatch and
    falls back to a default value if none are found.
    """
    available_fields = get_concrete_fields(PlayerMatch)
    for col in TIMING_CANDIDATE_COLS:
        if col in available_fields:
            # Cast `seconds / 60` to an integer to get minute buckets.
            return Cast(F(col) / Value(60), output_field=IntegerField())
    # Fallback if no timing column exists.
    return Value(0, output_field=IntegerField())


def _get_lane_role_expression() -> Expression:
    """
    Dynamically creates an ORM expression to determine lane role.
    If a 'lane_role' field exists, it's used directly. Otherwise, it derives
    a coarse role from the 'player_slot' field using a predefined mapping.
    """
    available_fields = get_concrete_fields(PlayerMatch)
    if "lane_role" in available_fields:
        return F("lane_role")

    # Fallback: derive role from player_slot.
    return Case(
        When(player_slot__in=LANE_ROLE_MAPPING[1], then=Value(1)),  # Cores
        When(player_slot__in=LANE_ROLE_MAPPING[4], then=Value(4)),  # Supports
        default=Value(0),  # Unknown/Other
        output_field=SmallIntegerField(),
    )


def refresh_lane_scenarios(epoch_week: int) -> None:
    """
    Aggregates and upserts (hero, lane_role, time_bucket) win/game counts
    into the Scenario table for a given week.
    """

    # 1. Prepare dynamic expressions for time and lane role.
    time_bucket_expr = _get_time_bucket_expression()
    lane_role_expr = _get_lane_role_expression()

    # 2. Aggregate game and win counts for each scenario.
    scenario_stats = (
        PlayerMatch.objects.annotate(time_bucket=time_bucket_expr, derived_lane_role=lane_role_expr)
        .values("hero_id", "derived_lane_role", "time_bucket")
        .annotate(
            games=Count("match_id", distinct=True),
            wins=Sum(
                Case(
                    When(
                        Q(is_radiant=True, match__winner=0) | Q(is_radiant=False, match__winner=1),
                        then=1,
                    ),
                    default=0,
                    output_field=IntegerField(),
                ),
            ),
        )
    )

    # 3. Prepare Scenario objects for bulk upsert.
    batch = [
        Scenario(
            hero_id=row["hero_id"],
            lane_role=row["derived_lane_role"],
            time=row["time_bucket"],
            games=row["games"],
            wins=row["wins"],
            epoch_week=epoch_week,
        )
        for row in scenario_stats.iterator()
    ]

    if not batch:
        return

    # 4. Perform a bulk upsert operation.
    # `update_conflicts` makes this an efficient "upsert" query.
    Scenario.objects.bulk_create(
        batch,
        batch_size=2000,
        update_conflicts=True,
        update_fields=["games", "wins"],
        unique_fields=["hero_id", "lane_role", "time", "epoch_week"],
    )


# ─── Command Wrapper ──────────────────────────────────────────────────────────


class Command(BaseCommand):
    """
    Management command to parse the database and populate HeroRanking and Scenario tables.
    """

    help = "Refreshes hero rankings and win-rate scenarios from match data."

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("► Starting stats refresh process..."))
        epoch_week = get_current_epoch_week()

        # Execute the refresh functions.
        # Note: An 'item_scenarios' function could be added here following the
        # same pattern as 'lane_scenarios'.
        refresh_hero_rankings()
        refresh_lane_scenarios(epoch_week)

        self.stdout.write(self.style.SUCCESS("✓ Stats refresh process completed successfully."))
