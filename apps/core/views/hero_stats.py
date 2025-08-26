# apps/core/views/hero_stats.py
from __future__ import annotations

import dataclasses
import heapq
from collections import defaultdict, namedtuple
from itertools import combinations
from typing import TYPE_CHECKING, Any

import structlog
from asgiref.sync import sync_to_async
from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Count, ExpressionWrapper, F, FloatField, Q
from django.utils import timezone

# --- REFACTORED: Use centralized utilities ---
from apps.core.conf import TIMEOUTS
from apps.core.models import Hero
from apps.core.utils import (
    apply_scope_filter,
    build_scope_tables,
    get_meta_recommendations,
    recommend,
    resolve_scope,
)
from apps.matches.models import PickBan
from common.cache_utils import aget_json, aset_json
from common.views_utils import BaseAsyncView, OrjsonResponse

if TYPE_CHECKING:
    from django.http import HttpRequest

log = structlog.get_logger(__name__).bind(component="heroViews")


# ────────────────────────────────────────────────────────────────────
#  TYPE ALIASES
# ────────────────────────────────────────────────────────────────────

type PairwiseTable = dict[tuple[int, int], float]
type RecommendationHeap = list[tuple[float, int]]
type DraftState = dict[str, set[int]]


# ────────────────────────────────────────────────────────────────────
#  SHARED HERO MAP CACHE (module-level)
# ────────────────────────────────────────────────────────────────────

_hero_map_cache: dict[int, str] | None = None


@sync_to_async
def _get_hero_map_sync(hero_ids: list[int] | None = None) -> dict[int, str]:
    global _hero_map_cache
    if _hero_map_cache is None:
        _hero_map_cache = {
            h["id"]: h["localized_name"]
            for h in Hero.objects.all().values("id", "localized_name")
        }
    if hero_ids is not None:
        return {hid: _hero_map_cache.get(hid, "Unknown") for hid in hero_ids}
    return _hero_map_cache

def clear_hero_map_cache() -> None:
    """Call after updating Hero objects."""
    global _hero_map_cache
    _hero_map_cache = None


# ────────────────────────────────────────────────────────────────────
#  CORE ABSTRACTION: THE UNIVERSAL BASE VIEW
# ────────────────────────────────────────────────────────────────────

@dataclasses.dataclass(frozen=True)
class StatFilters:
    """A structured container for all possible request filters."""
    patch_id: str | None
    rank_tier: str | None
    league_id: str | None
    team_id: str | None
    player_id: str | None
    min_games: int
    top_count: int


class BaseScopedStatsView(BaseAsyncView):
    """
    An abstract base view that standardizes request parsing, caching, and
    response formatting for all statistics endpoints.
    """
    ENDPOINT_CACHE_KEY: str

    async def get(self, request: HttpRequest, **kwargs) -> OrjsonResponse:
        """Universal GET handler."""
        try:
            p = StatFilters(
                patch_id=request.GET.get("patch_id"),
                rank_tier=request.GET.get("rank_tier"),
                league_id=request.GET.get("league_id"),
                team_id=request.GET.get("team_id"),
                player_id=request.GET.get("player_id"),
                min_games=self.get_int_param(request, "min_games", default=10),
                top_count=self.get_int_param(request, "top_count", default=20),
            )

            async def _producer() -> dict[str, Any]:
                stats_data = await self._produce_stats(p)
                return {
                    **stats_data,
                    "metadata": {
                        "filters": {k: v for k, v in dataclasses.asdict(p).items() if v not in (None, 0)},
                        "generated_at": timezone.now().isoformat(timespec="seconds"),
                    },
                }

            data = await self.get_cached_data(
                request,
                producer=_producer,
                ttl=TIMEOUTS.get(self.ENDPOINT_CACHE_KEY, 300),
                **{k: v for k, v in dataclasses.asdict(p).items() if v is not None},
            )
            return OrjsonResponse(data)

        except Exception as exc:
            log.exception("View failed", exc_info=exc)
            return OrjsonResponse({"error": "Internal server error"}, status=500)

    async def _produce_stats(self, p: StatFilters) -> dict[str, Any]:
        raise NotImplementedError


# ────────────────────────────────────────────────────────────────────
#  INTERMEDIATE BASE VIEWS
# ────────────────────────────────────────────────────────────────────

class BaseHeroStatView(BaseScopedStatsView):
    """Base for single-hero stats (picks/bans)."""
    MODEL_FIELD: dict
    TITLE: str

    async def _produce_stats(self, p: StatFilters) -> dict[str, Any]:
        filters = Q(**self.MODEL_FIELD)
        if p.patch_id:
            filters &= Q(match__patch_id=p.patch_id)
        if p.rank_tier:
            filters &= Q(match__rank_tier=p.rank_tier)

        filters = apply_scope_filter(filters, league_id=p.league_id, team_id=p.team_id, player_id=p.player_id)

        qs = (
            PickBan.objects.filter(filters)
            .values("hero_id")
            .annotate(
                games=Count("match", distinct=True),
                wins=Count("match", filter=Q(team=F("match__winner")), distinct=True),
            )
            .filter(games__gte=p.min_games)
            .annotate(win_rate=ExpressionWrapper(100.0 * F("wins") / F("games"), output_field=FloatField()))
            .order_by("-win_rate", "-games")
        )

        rows = [row async for row in qs]
        hero_ids = [r["hero_id"] for r in rows]
        hero_map = await _get_hero_map_sync(hero_ids)  # ✅ Fixed: shared function

        result_list = [
            {
                "hero": {"id": r["hero_id"], "name": hero_map.get(r["hero_id"], "Unknown")},
                "stats": {
                    "games": r["games"],
                    "wins": r["wins"],
                    "losses": r["games"] - r["wins"],
                    "win_rate": round(r["win_rate"], 2),
                },
            }
            for r in rows
        ]
        return {self.TITLE: result_list}


class BaseHeroGroupingStatsView(BaseScopedStatsView):
    """Base for hero combination stats."""

    async def _produce_stats(self, p: StatFilters) -> dict[str, Any]:
        filters = Q(is_pick=True)
        if p.patch_id:
            filters &= Q(match__patch_id=p.patch_id)
        if p.rank_tier:
            filters &= Q(match__rank_tier=p.rank_tier)

        filters = apply_scope_filter(filters, league_id=p.league_id, team_id=p.team_id, player_id=p.player_id)

        picks = (
            PickBan.objects.filter(filters)
            .values("match_id", "team", "match__winner")
            .annotate(heroes=ArrayAgg("hero_id", distinct=True))
            .filter(heroes__len__gte=5)
            .order_by("match_id")
        )

        GroupStats = namedtuple("GroupStats", "wins total")
        counters = {size: defaultdict(lambda: GroupStats(0, 0)) for size in (2, 3, 4, 5)}

        async for row in picks:
            heroes, won = sorted(row["heroes"][:5]), row["match__winner"] == row["team"]
            for size, store in counters.items():
                if len(heroes) < size:
                    continue
                for combo in combinations(heroes, size):
                    prev = store[combo]
                    store[combo] = GroupStats(prev.wins + won, prev.total + 1)

        hero_ids = {hid for d in counters.values() for combo in d for hid in combo}
        hero_map = await _get_hero_map_sync(list(hero_ids))  # ✅ Fixed

        return {
            "duos": await self._get_top_n_combos(counters[2], p.top_count, hero_map, min_games=p.min_games),
            "trios": await self._get_top_n_combos(counters[3], p.top_count, hero_map, min_games=p.min_games),
            "quads": await self._get_top_n_combos(counters[4], p.top_count, hero_map, min_games=p.min_games),
            "five_stacks": await self._get_top_n_combos(counters[5], p.top_count, hero_map, min_games=p.min_games),
        }

    async def _get_top_n_combos(
        self, size_dict: dict, top_count: int, hero_map: dict, min_games: int,
    ) -> list:
        heap = []
        for combo, stats in size_dict.items():
            if stats.total < min_games:
                continue
            win_rate = stats.wins * 100 / stats.total
            node = (-win_rate, -stats.total, combo, stats)
            if len(heap) < top_count:
                heapq.heappush(heap, node)
            else:
                heapq.heappushpop(heap, node)

        sorted_heap = sorted(heap, key=lambda x: (x[0], x[1]))
        return [
            {
                "rank": i,
                "heroes": [{"id": h_id, "name": hero_map[h_id]} for h_id in combo],
                "stats": {
                    "games": stats.total,
                    "wins": stats.wins,
                    "losses": stats.total - stats.wins,
                    "win_rate": round(-win_rate, 2),
                },
            }
            for i, (win_rate, _, combo, stats) in enumerate(sorted_heap, 1)
        ]


# ────────────────────────────────────────────────────────────────────
#  CONCRETE VIEWS
# ────────────────────────────────────────────────────────────────────

class HeroPickStatsView(BaseHeroStatView):
    ENDPOINT_CACHE_KEY = "hero_pick_stats"
    MODEL_FIELD = {"is_pick": True}
    TITLE = "picks"


class HeroBanStatsView(BaseHeroStatView):
    ENDPOINT_CACHE_KEY = "hero_ban_stats"
    MODEL_FIELD = {"is_pick": False}
    TITLE = "bans"


class HeroGroupingStatsView(BaseHeroGroupingStatsView):
    ENDPOINT_CACHE_KEY = "hero_grouping_stats"


class ScopedHeroRecommendView(BaseScopedStatsView):
    ENDPOINT_CACHE_KEY = "hero_recommendations"

    async def _produce_stats(self, p: StatFilters) -> dict[str, Any]:
        try:
            draft = await self._parse_and_validate_draft()
        except ValueError as e:
            return {"error": str(e)}

        if not draft["allies"] and not draft["enemies"]:
            return await get_meta_recommendations(filters=None, min_games=p.min_games)

        scope_key, filters = resolve_scope(
            league_id=int(p.league_id) if p.league_id else None,
            team_id=int(p.team_id) if p.team_id else None,
            player_id=int(p.player_id) if p.player_id else None,
        )

        synergy, counter = await self._load_recommendation_tables(
            scope_key=scope_key,
            filters=filters,
            min_games=p.min_games,
        )

        pick_heap, ban_heap = recommend(
            allies=draft["allies"],
            enemies=draft["enemies"],
            banned=draft["banned"],
            synergy=synergy,
            counter=counter,
            top=p.top_count,
        )

        return await self._format_response(draft, pick_heap, ban_heap)

    async def _parse_and_validate_draft(self) -> DraftState:
        def _parse_ids(param_name: str) -> set[int]:
            value = self.request.GET.get(param_name, "")
            return {int(id_str) for id_str in value.split(",") if id_str.strip().isdigit()} if value else set()

        legacy_allies = {int(v) for k, v in self.request.GET.items() if k.startswith("hero_") and v.isdigit()}

        draft = {
            "allies": _parse_ids("allies").union(legacy_allies),
            "enemies": _parse_ids("enemies"),
            "banned": _parse_ids("banned"),
        }
        all_ids = draft["allies"].union(draft["enemies"], draft["banned"])

        if all_ids:
            # ✅ Fixed: Correct way to get valid hero IDs
            valid_id_tuples = await sync_to_async(list)(
                Hero.objects.filter(id__in=all_ids).values_list("id", flat=True),
            )
            valid_ids = set(valid_id_tuples)
            if invalid_ids := all_ids - valid_ids:
                msg = f"Invalid hero IDs: {sorted(invalid_ids)}"
                raise ValueError(msg)

        return draft

    async def _load_recommendation_tables(
        self,
        *,
        scope_key: str,
        filters: Q,
        min_games: int,
    ) -> tuple[PairwiseTable, PairwiseTable]:
        raw_cache_key = f"hero:recommend:raw:{scope_key}"
        if raw_cached := await aget_json(raw_cache_key):
            return self._hydrate_tables_with_min_games(raw_cached, min_games)

        if scope_key == "global":
            global_raw = await aget_json("hero:recommend:raw:global")
            if global_raw:
                return self._hydrate_tables_with_min_games(global_raw, min_games)

        synergy, counter = await build_scope_tables(filters, min_games=1)
        cache_key = f"hero:recommend:raw:{scope_key}"
        await aset_json(
            cache_key,
            {
                "synergy": {f"{a},{b}": round(v, 2) for (a, b), v in synergy.items()},
                "counter": {f"{a},{b}": round(v, 2) for (a, b), v in counter.items()},
            },
            ttl=86400,
        )
        filtered_syn = {k: v for k, v in synergy.items() if v >= min_games}
        filtered_ctr = {k: v for k, v in counter.items() if v >= min_games}
        return filtered_syn, filtered_ctr

    def _hydrate_tables_with_min_games(self, raw_data: dict, min_games: int) -> tuple[PairwiseTable, PairwiseTable]:
        def _to_table(data: dict) -> PairwiseTable:
            table = {}
            for k, v in data.items():
                try:
                    a, b = map(int, k.split(","))
                    if v >= min_games:
                        table[(a, b)] = float(v)
                except (ValueError, TypeError):
                    log.warning("Skipping malformed raw table entry", key=k)
            return table

        return _to_table(raw_data.get("synergy", {})), _to_table(raw_data.get("counter", {}))

    async def _format_response(
        self,
        draft: DraftState,
        pick_heap: RecommendationHeap,
        ban_heap: RecommendationHeap,
    ) -> dict[str, Any]:
        all_ids = {h for _, h in pick_heap + ban_heap} | draft["allies"] | draft["enemies"] | draft["banned"]
        hero_map = await _get_hero_map_sync(list(all_ids))

        def _fmt_hero(hid: int):
            return {"id": hid, "name": hero_map.get(hid, "Unknown")}

        def _fmt_recs(heap: RecommendationHeap):
            sorted_heap = sorted(heap, key=lambda x: x[0], reverse=True)
            return [
                {"rank": i, "hero": _fmt_hero(hid), "score": round(score, 2)}
                for i, (score, hid) in enumerate(sorted_heap, 1)
            ]

        return {
            "input": {
                "allies": [_fmt_hero(h) for h in sorted(draft["allies"])],
                "enemies": [_fmt_hero(h) for h in sorted(draft["enemies"])],
                "banned": [_fmt_hero(h) for h in sorted(draft["banned"])],
            },
            "best_picks": _fmt_recs(pick_heap),
            "best_bans": _fmt_recs(ban_heap),
        }
