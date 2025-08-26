# services/heroes.py
"""
This module contains the core business logic for hero recommendations, including
on-the-fly table generation and draft-based scoring.
"""

import heapq
from collections import Counter, namedtuple
from collections.abc import Mapping
from itertools import combinations
from typing import Any

from asgiref.sync import sync_to_async
from django.contrib.postgres.aggregates.general import ArrayAgg
from django.db.models import Q
from django.db.models.aggregates import Count, Sum
from django.db.models.expressions import Case, F, When
from django.db.models.fields import FloatField, IntegerField
from django.db.models.functions.comparison import Cast
from django.utils import timezone

from apps.core.models import Hero
from apps.matches.models import PickBan

Stats = namedtuple("Stats", "wins games")


def _pairwise_win_rates_optimized(
    rows: list[tuple[int, int, int, list[int]]],
    min_games: int,
) -> tuple[dict[tuple[int, int], float], dict[tuple[int, int], float]]:
    """
    Optimized version using Counters for bulk updates.
    """
    synergy_wins = Counter()
    synergy_total = Counter()
    counter_wins = Counter()
    counter_total = Counter()

    i = 0
    n = len(rows)

    while i < n:
        match_id = rows[i][0]
        sides = {}
        winner = -1

        # Collect all rows for this match_id
        while i < n and rows[i][0] == match_id:
            _, team, match_winner, heroes = rows[i]
            sides[team] = tuple(sorted(heroes[:5]))  # Ensure consistent order
            winner = match_winner
            i += 1

        if len(sides) != 2 or winner not in (0, 1):
            continue

        radiant_heroes, dire_heroes = sides.get(1, ()), sides.get(0, ())
        if not radiant_heroes or not dire_heroes:
            continue

        radiant_won = winner == 1

        # --- Synergy: within teams ---
        for heroes, team_id in [(radiant_heroes, 1), (dire_heroes, 0)]:
            won = winner == team_id
            for pair in combinations(heroes, 2):
                key = tuple(sorted(pair))  # canonical order
                synergy_total[key] += 1
                if won:
                    synergy_wins[key] += 1

        # --- Counters: across teams ---
        for r_hero in radiant_heroes:
            for d_hero in dire_heroes:
                # Radiant vs Dire
                r_key = (r_hero, d_hero)
                counter_total[r_key] += 1
                if radiant_won:
                    counter_wins[r_key] += 1

                # Dire vs Radiant
                d_key = (d_hero, r_hero)
                counter_total[d_key] += 1
                if not radiant_won:
                    counter_wins[d_key] += 1

    # Final win rates
    synergy = {
        pair: (synergy_wins[pair] * 100.0 / synergy_total[pair])
        for pair in synergy_total
        if synergy_total[pair] >= min_games
    }

    counter = {
        pair: (counter_wins[pair] * 100.0 / counter_total[pair])
        for pair in counter_total
        if counter_total[pair] >= min_games
    }

    return synergy, counter


def recommend(
    allies: set[int],
    enemies: set[int],
    banned: set[int],
    *,
    synergy: Mapping[tuple[int, int], float],
    counter: Mapping[tuple[int, int], float],
    top: int = 20,
) -> tuple[list[tuple[float, int]], list[tuple[float, int]]]:
    """
    Recommends picks and bans based on a draft state using synergy and counter data.

    This refactored version uses a helper function to reduce repetition and
    correctly identifies all possible hero candidates from both tables.
    """

    # Helper to calculate the average score for a hero against a list of others.
    def _get_avg_score(
        hero_id: int,
        other_heroes: set[int],
        table: Mapping[tuple[int, int], float],
        is_synergy: bool = False,
    ) -> float:
        if not other_heroes:
            return 50.0  # Return a neutral score if there's nothing to compare against

        scores = []
        for other_id in other_heroes:
            key = tuple(sorted((hero_id, other_id))) if is_synergy else (hero_id, other_id)
            scores.append(table.get(key, 50.0))
        return sum(scores) / len(scores)

    # **FIX**: Get all hero IDs from *both* synergy and counter tables.
    all_hero_ids = {h for pair in synergy for h in pair}.union({h for pair in counter for h in pair})

    # Candidates are any heroes not already picked or banned.
    picked_or_banned = allies.union(enemies, banned)
    candidate_ids = all_hero_ids - picked_or_banned

    pick_heap: list[tuple[float, int]] = []
    ban_heap: list[tuple[float, int]] = []

    for hero_id in candidate_ids:
        # --- Score Calculation (Refactored for clarity) ---
        ally_synergy_score = _get_avg_score(hero_id, allies, synergy, is_synergy=True)
        enemy_counter_score = _get_avg_score(hero_id, enemies, counter)
        threat_score = _get_avg_score(hero_id, allies, counter)
        enemy_synergy_score = _get_avg_score(hero_id, enemies, synergy, is_synergy=True)

        # --- Combine Scores ---
        # A good pick works well with allies AND/OR counters enemies.
        pick_scores = []
        if allies:
            pick_scores.append(ally_synergy_score)
        if enemies:
            pick_scores.append(enemy_counter_score)
        pick_score = sum(pick_scores) / len(pick_scores) if pick_scores else 50.0

        # A good ban is a threat to allies AND/OR works well with enemies.
        ban_scores = []
        if allies:
            ban_scores.append(threat_score)
        if enemies:
            ban_scores.append(enemy_synergy_score)
        ban_score = sum(ban_scores) / len(ban_scores) if ban_scores else 50.0

        # Use heaps to efficiently track the top N recommendations.
        if len(pick_heap) < top:
            heapq.heappush(pick_heap, (pick_score, hero_id))
            heapq.heappush(ban_heap, (ban_score, hero_id))
        else:
            heapq.heappushpop(pick_heap, (pick_score, hero_id))
            heapq.heappushpop(ban_heap, (ban_score, hero_id))

    # The heaps store the *lowest* scores, so we return them as-is.
    # The caller is responsible for sorting them descending for presentation.
    return pick_heap, ban_heap


async def build_scope_tables(
    filters: Q,
    *,
    min_games: int = 1,  # now ignored; used later
) -> tuple[dict[tuple[int, int], float], dict[tuple[int, int], float]]:
    qs = (
        PickBan.objects.filter(filters, is_pick=True)
        .values("match_id", "team", "match__winner")
        .annotate(heroes=ArrayAgg("hero_id", distinct=True))
        .filter(heroes__len=5)
        .order_by("match_id")
        .values_list("match_id", "team", "match__winner", "heroes")
    )

    rows = await sync_to_async(list, thread_sensitive=False)(qs)
    return await sync_to_async(_pairwise_win_rates_optimized, thread_sensitive=False)(rows, min_games)


async def get_meta_recommendations(
    *,
    filters: Q | None,
    min_games: int,
) -> dict[str, Any]:
    """Calculates a general "meta" report when the input draft is empty."""
    META_RECOMMENDATION_LIMIT: int = 1000
    base_query = PickBan.objects.filter(is_pick=True)
    if filters:
        base_query = base_query.filter(filters)

    hero_stats_qs = (
                        base_query.values("hero_id")
                        .annotate(
                            games=Count("id"),
                            wins=Sum(
                                Case(When(match__winner=F("team"), then=1), default=0, output_field=IntegerField())),
                        )
                        .filter(games__gte=min_games)
                        .annotate(
                            win_rate=Cast(F("wins"), FloatField()) * 100 / Cast(F("games"), FloatField()),
                        )
                        .order_by("-win_rate", "-games")
                        .values("hero_id", "win_rate", "games", "wins")
                    )[:META_RECOMMENDATION_LIMIT]

    hero_stats = await sync_to_async(list)(hero_stats_qs)
    hero_ids = [stat["hero_id"] for stat in hero_stats]
    hero_map = {h.id: h.localized_name for h in await sync_to_async(list)(Hero.objects.filter(id__in=hero_ids))}

    recs = [
        {
            "rank": i,
            "hero": {"id": stat["hero_id"], "name": hero_map.get(stat["hero_id"], "Unknown Hero")},
            "score": round(stat["win_rate"], 2),
            "stats": {"games": stat["games"], "wins": stat["wins"], "win_rate": round(stat["win_rate"], 2)},
        }
        for i, stat in enumerate(hero_stats, 1)
    ]

    return {
        "input": {"allies": [], "enemies": [], "banned": []},
        "best_picks": recs,
        "best_bans": recs,
        "generated_at": timezone.now().isoformat(timespec="seconds"),
    }


# The logic from ScopeFilterMixin is now a standalone, reusable function.
def apply_scope_filter(
    q: Q,
    *,
    league_id: str | None = None,
    team_id: str | None = None,
    player_id: str | None = None,
    match_id: str | None = None,
) -> Q:
    """Applies scope-based filters to a Q() object."""
    if league_id:
        q &= Q(match__league_id=league_id)
    if team_id:
        q &= Q(match__radiant_team_id=team_id) | Q(match__dire_team_id=team_id)
    if player_id:
        from apps.players.models import PlayerMatchHistory

        q &= Q(match_id__in=PlayerMatchHistory.objects.filter(player_id=player_id).values_list("match_id", flat=True))
    if match_id:
        q &= Q(match_id=match_id)
    return q


def resolve_scope(
    *,
    league_id: int | None = None,
    team_id: int | None = None,
    player_id: int | None = None,
) -> tuple[str, Q]:
    """
    Resolves scope identifiers into a cache key and a database filter.
    This is the single source of truth for scope resolution, used by both
    API views and management commands.
    It checks for scopes in order of priority (most specific first).
    """
    filters = Q()

    if player_id:
        scope_key = f"player:{player_id}"
        filters = apply_scope_filter(filters, player_id=str(player_id))
    elif team_id:
        scope_key = f"team:{team_id}"
        filters = apply_scope_filter(filters, team_id=str(team_id))
    elif league_id:
        scope_key = f"league:{league_id}"
        filters = apply_scope_filter(filters, league_id=str(league_id))
    else:
        # Default to the global scope if no specific ID is provided.
        scope_key = "global"

    return scope_key, filters
