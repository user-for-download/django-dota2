# apps/core/management/commands/build_pairwise_table.py

from __future__ import annotations

import asyncio
import json
from collections import Counter
from itertools import combinations
from typing import TYPE_CHECKING, Any

import structlog
from django.contrib.postgres.aggregates.general import ArrayAgg
from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.db.models import Q

from apps.core.models import Hero
from apps.core.utils import resolve_scope
from apps.matches.models import PickBan
from common.cache_utils import aset_json

if TYPE_CHECKING:
    from collections.abc import Mapping


log = structlog.get_logger(__name__)

# Default TTL: 24 hours
DEFAULT_CACHE_TTL_SECONDS = 60 * 60 * 24  # 24h

# Cache key for global hero map
HERO_MAP_CACHE_KEY = "hero:map"


def _pairwise_win_rates_optimized(
    rows: list[tuple[int, int, int, list[int]]],
    min_games: int = 1,
) -> tuple[dict[tuple[int, int], float], dict[tuple[int, int], float]]:
    """
    Compute synergy and counter win rates using optimized counting.

    Args:
        rows: List of (match_id, team, winner, [hero_ids])
        min_games: Minimum number of games to include a pair (used at final step)

    Returns:
        (synergy_table, counter_table) as { (hero_a, hero_b): win_rate }
    """
    synergy_wins = Counter()
    synergy_total = Counter()
    counter_wins = Counter()
    counter_total = Counter()

    i = 0
    n = len(rows)

    while i < n:
        match_id = rows[i][0]
        sides: dict[int, tuple[int, ...]] = {}
        winner = -1

        # Collect all rows for this match
        while i < n and rows[i][0] == match_id:
            _, team, match_winner, heroes = rows[i]
            # Ensure exactly 5 heroes, sorted
            valid_heroes = [h for h in heroes if h]
            if len(valid_heroes) >= 5:
                sides[team] = tuple(sorted(valid_heroes[:5]))
            winner = match_winner
            i += 1

        if len(sides) != 2 or winner not in (0, 1):
            continue  # Skip incomplete matches

        radiant_heroes = sides.get(1, ())
        dire_heroes = sides.get(0, ())
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

        # --- Counter: across teams ---
        for r_hero in radiant_heroes:
            for d_hero in dire_heroes:
                # Radiant hero vs Dire hero
                r_key = (r_hero, d_hero)
                counter_total[r_key] += 1
                if radiant_won:
                    counter_wins[r_key] += 1

                # Dire hero vs Radiant hero
                d_key = (d_hero, r_hero)
                counter_total[d_key] += 1
                if not radiant_won:
                    counter_wins[d_key] += 1

    # Final win rates (only for pairs meeting min_games)
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


async def _get_hero_map() -> dict[int, str]:
    """
    Get hero ID → localized_name map with memoization.
    Call clear_hero_map_cache() after hero updates.
    """
    if hasattr(_get_hero_map, "_cache"):
        return _get_hero_map._cache

    hero_map = {
        h.id: h.localized_name
        for h in await Hero.objects.all().adefer("roles").values_list("id", "localized_name")
    }
    _get_hero_map._cache = hero_map
    return hero_map


def clear_hero_map_cache() -> None:
    """Call after updating Hero objects."""
    if hasattr(_get_hero_map, "_cache"):
        delattr(_get_hero_map, "_cache")


class Command(BaseCommand):
    help = "Build hero synergy/counter tables and cache them in Redis."

    def add_arguments(self, parser: CommandParser) -> None:
        scope = parser.add_mutually_exclusive_group(required=True)
        scope.add_argument("--global", action="store_true", dest="global_scope", help="Build tables for all matches.")
        scope.add_argument("--team", type=int, help="Build tables for a single team_id.")
        scope.add_argument("--player", type=int, help="Build tables for a single account_id.")
        scope.add_argument("--league", type=int, help="Build tables for a single league_id.")

        parser.add_argument(
            "--min-games",
            type=int,
            default=10,
            help="Minimum number of games a pair must have to be included in output (default: 10).",
        )
        parser.add_argument(
            "--json",
            action="store_true",
            help="Output raw JSON summary instead of pretty text.",
        )

    def handle(self, *args: Any, **opts: Any) -> None:
        try:
            asyncio.run(self._handle_async(**opts))
        except KeyboardInterrupt:
            self.stderr.write(self.style.WARNING("\nOperation cancelled by user."))
        except Exception as exc:
            log.exception("build_pairwise_table failed.", exc_info=exc)
            raise CommandError(str(exc)) from exc

    async def _handle_async(self, **opts: Any) -> None:
        requested_min_games = opts["min_games"]

        scope_key, filters = resolve_scope(
            team_id=opts.get("team"),
            player_id=opts.get("player"),
            league_id=opts.get("league"),
        )
        is_global = scope_key == "global"

        self.stdout.write(
            self.style.SUCCESS(
                f"► Building '{scope_key}' hero synergy/counter tables (output min_games={requested_min_games})...",
            ),
        )

        # Build SQL query with early filtering
        qs = (
            PickBan.objects.filter(filters, is_pick=True)
            .values("match_id", "team", "match__winner")
            .annotate(heroes=ArrayAgg("hero_id", distinct=True, filter=~Q(hero_id__isnull=True)))
            .filter(heroes__len__gte=5)  # Only full teams
            .order_by("match_id")
            .values_list("match_id", "team", "match__winner", "heroes")
        )

        # Fetch all rows
        rows = [row async for row in qs]

        if not rows:
            self.stdout.write(self.style.WARNING("No match data found for the given scope."))
            return

        self.stdout.write(f"Processing {len(rows)} match records...")

        # Compute synergy and counter tables
        syn, ctr = _pairwise_win_rates_optimized(rows, min_games=1)  # Always compute all, filter later

        # Serialize for Redis: use "a,b" format
        def _serialize(table: dict[tuple[int, int], float]) -> dict[str, float]:
            return {f"{a},{b}": round(v, 2) for (a, b), v in table.items()}

        # Cache raw counters (for future reuse with different min_games)
        if is_global:
            # Save raw counters for global scope
            await aset_json(
                "hero:recommend:raw:global",
                {
                    "synergy": _serialize(syn),
                    "counter": _serialize(ctr),
                },
                ttl=DEFAULT_CACHE_TTL_SECONDS,
            )

            # Also save filtered output for immediate use
            filtered_syn = {k: v for k, v in syn.items() if v >= requested_min_games}
            filtered_ctr = {k: v for k, v in ctr.items() if v >= requested_min_games}

            await aset_json(
                "hero:synergy:pairwise",
                _serialize(filtered_syn),
                ttl=DEFAULT_CACHE_TTL_SECONDS,
            )
            await aset_json(
                "hero:counter:pairwise",
                _serialize(filtered_ctr),
                ttl=DEFAULT_CACHE_TTL_SECONDS,
            )
        else:
            # For scoped views, cache both raw and final
            raw_key = f"hero:recommend:raw:{scope_key}"
            cache_key = f"hero:recommend:{scope_key}:mg={requested_min_games}"

            await aset_json(
                raw_key,
                {
                    "synergy": _serialize(syn),
                    "counter": _serialize(ctr),
                },
                ttl=DEFAULT_CACHE_TTL_SECONDS,
            )

            # Filter now for requested min_games
            filtered_syn = {k: v for k, v in syn.items() if v >= requested_min_games}
            filtered_ctr = {k: v for k, v in ctr.items() if v >= requested_min_games}

            await aset_json(
                cache_key,
                {
                    "syn": _serialize(filtered_syn),
                    "ctr": _serialize(filtered_ctr),
                },
                ttl=DEFAULT_CACHE_TTL_SECONDS,
            )

        # Output Summary
        total_pairs = len(syn) + len(ctr)
        final_syn = len([v for v in syn.values() if v >= requested_min_games])
        final_ctr = len([v for v in ctr.values() if v >= requested_min_games])

        if opts["json"]:
            out: Mapping[str, Any] = {
                "scope": scope_key,
                "min_games": requested_min_games,
                "total_pairs_processed": total_pairs,
                "synergy_pairs": final_syn,
                "counter_pairs": final_ctr,
                "matches_processed": len({row[0] for row in rows}),
            }
            self.stdout.write(json.dumps(out, indent=2))
        else:
            s = self.style
            self.stdout.write(s.MIGRATE_HEADING("\n✓ BUILD SUMMARY"))
            self.stdout.write(f"  Scope               : {scope_key}")
            self.stdout.write(f"  Min Games (output)  : {requested_min_games}")
            self.stdout.write(f"  Matches Processed   : {len({row[0] for row in rows})}")
            self.stdout.write(f"  Total Pairs Found   : {total_pairs}")
            self.stdout.write(f"  Synergy Pairs (≥{requested_min_games}) : {final_syn}")
            self.stdout.write(f"  Counter Pairs (≥{requested_min_games}) : {final_ctr}")
            self.stdout.write(s.MIGRATE_HEADING(""))

        self.stdout.write(self.style.SUCCESS("✓ Done. Tables stored in Redis."))
