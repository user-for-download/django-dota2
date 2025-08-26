# apps/matches/views.py (Refactored)
# =====================================================================
"""Async-first API views for the 'matches' application, fully refactored."""

from __future__ import annotations

import asyncio
from typing import Any

import structlog
from asgiref.sync import sync_to_async
from django.db.models.query_utils import Q
from django.http import Http404, HttpRequest
from django.utils import timezone

from apps.players.models import PlayerMatchHistory
from common.views_utils import BaseAppView

from .conf import TIMEOUTS
from .models import Match, PlayerMatch
from .serializers import MatchSerializer, PlayerMatchSerializer

log = structlog.get_logger(__name__).bind(component="MatchViews")


def _safe_int(raw: str | None, default: int = 0) -> int:
    try:
        return int(raw or default)
    except (TypeError, ValueError):
        return default


class FilteredMatchListView(BaseAppView):
    """
    A single, powerful view to list matches with extensive filtering capabilities.
    Replaces the old MatchListView, TeamMatchesView, LeagueMatchesView, etc.

    Handles: GET /api/v1/matches/
    Query Parameters:
    - Pagination: `page`, `page_size`
    - Scoping: `team_id`, `team_ids`, `league_id`, `league_ids`, `player_id`, `player_ids`
    - General: `min_duration`, `after_id`, `include_stats`, `include_patch`
    """

    CACHE_TTL = TIMEOUTS["match_list"]

    def _parse_int_list(self, request: HttpRequest, param_name: str) -> list[int] | None:
        """Helper to parse both singular and comma-separated integer parameters."""
        value = request.GET.get(param_name) or request.GET.get(f"{param_name}s")  # Handles team_id and team_ids
        if not value:
            return None
        try:
            return [int(i.strip()) for i in value.split(",") if i.strip().isdigit()]
        except (ValueError, TypeError):
            return None

    def _get_params(self, request: HttpRequest, **kwargs) -> dict[str, Any]:
        """Gathers all possible filtering and pagination parameters."""
        page = self.get_page(request)
        return {
            "page_num": page.number,
            "page_size": page.size,
            "offset": page.offset,
            "inc_stats": self.get_bool_param(request, "include_stats"),
            "inc_patch": self.get_bool_param(request, "include_patch"),
            "after_id": int(request.GET.get("after_id", 0)),
            "min_dur": int(request.GET.get("min_duration", 0)),
            "team_ids": self._parse_int_list(request, "team_id"),
            "league_ids": self._parse_int_list(request, "league_id"),
            "player_ids": self._parse_int_list(request, "player_id"),
        }

    async def _produce_payload(self, p: dict[str, Any]) -> dict[str, Any]:
        """Builds the query and serializes the data based on the parsed parameters."""
        # 1. Build the filter Q object from scope parameters
        filters = Q()
        if team_ids := p.get("team_ids"):
            filters &= Q(radiant_team_id__in=team_ids) | Q(dire_team_id__in=team_ids)
        if league_ids := p.get("league_ids"):
            filters &= Q(league_id__in=league_ids)
        if player_ids := p.get("player_ids"):
            player_match_ids = PlayerMatchHistory.objects.filter(
                player_id__in=player_ids,
            ).values_list("match_id", flat=True)
            filters &= Q(match_id__in=list(await sync_to_async(list)(player_match_ids)))

        # 2. Add general filters
        if p["after_id"]:
            filters &= Q(match_id__gt=p["after_id"])
        if p["min_dur"]:
            filters &= Q(duration__gte=p["min_dur"])

        # 3. Construct and execute the main query
        qs = MatchSerializer.base_queryset(stats=p["inc_stats"], patch=p["inc_patch"])
        if filters:
            qs = qs.filter(filters)

        total = await qs.acount()
        matches = [m async for m in qs.order_by("-start_time")[p["offset"] : p["offset"] + p["page_size"]]]

        return {
            "count": total,
            "page": p["page_num"],
            "page_size": p["page_size"],
            "total_pages": -(-total // p["page_size"]) if p["page_size"] else 0,
            "data": MatchSerializer.serialize_matches(
                matches,
                include_stats=p["inc_stats"],
                include_patch=p["inc_patch"],
            ),
        }


class MatchListView(BaseAppView):
    """GET /api/v1/matches/"""

    CACHE_TTL = TIMEOUTS["match_list"]

    def _get_params(self, request: HttpRequest, **kwargs) -> dict[str, Any]:
        page = self.get_page(request)
        return {
            "page_num": page.number,
            "page_size": page.size,
            "offset": page.offset,
            "inc_stats": self.get_bool_param(request, "include_stats"),
            "inc_patch": self.get_bool_param(request, "include_patch"),
            "after_id": _safe_int(request.GET.get("after_id")),
            "min_dur": _safe_int(request.GET.get("min_duration")),
        }

    async def _produce_payload(self, p: dict[str, Any]) -> dict[str, Any]:
        qs = MatchSerializer.base_queryset(stats=p["inc_stats"], patch=p["inc_patch"], players=False)
        if p["after_id"]:
            qs = qs.filter(match_id__gt=p["after_id"])
        if p["min_dur"]:
            qs = qs.filter(duration__gte=p["min_dur"])

        total = await qs.acount()
        fields = ["match_id", "start_time", "duration", "winner", "radiant_score", "dire_score"]
        if p["inc_stats"]:
            fields += ["stats__tower_status_radiant", ...]
        if p["inc_patch"]:
            fields += ["patch_info__patch"]

        qs = Match.objects.values(*fields).order_by("-start_time")

        return {
            "count": total,
            "page": p["page_num"],
            "page_size": p["page_size"],
            "total_pages": -(-total // p["page_size"]) if p["page_size"] else 0,
            "data": MatchSerializer.serialize_matches(
                matches,
                include_stats=p["inc_stats"],
                include_patch=p["inc_patch"],
            ),
        }


class MatchDetailView(BaseAppView):
    """GET /api/v1/matches/{match_id}/"""

    CACHE_TTL = TIMEOUTS["match_detail"]

    def _get_params(self, request: HttpRequest, **kwargs) -> dict[str, Any]:
        return {
            "match_id": kwargs["match_id"],
            "inc_stats": self.get_bool_param(request, "include_stats", default=True),
            "inc_patch": self.get_bool_param(request, "include_patch", default=True),
            "inc_players": self.get_bool_param(request, "include_players", default=True),
        }

    async def _produce_payload(self, p: dict[str, Any]) -> dict[str, Any]:
        try:
            match = await MatchSerializer.base_queryset(
                stats=p["inc_stats"],
                patch=p["inc_patch"],
                players=p["inc_players"],
            ).aget(match_id=p["match_id"])
        except Match.DoesNotExist as e:
            msg = f"Match {p['match_id']} not found."
            raise Http404(msg) from e
        return MatchSerializer.serialize_match(
            match,
            include_stats=p["inc_stats"],
            include_patch=p["inc_patch"],
            include_players=p["inc_players"],
        )


class MatchPlayersView(BaseAppView):
    """GET /api/v1/matches/{match_id}/players/"""

    CACHE_TTL = TIMEOUTS["match_players"]

    def _get_params(self, request: HttpRequest, **kwargs) -> dict[str, Any]:
        return {
            "match_id": kwargs["match_id"],
            "include_hero": self.get_bool_param(request, "include_hero", default=True),
        }

    async def _produce_payload(self, p: dict[str, Any]) -> list[dict[str, Any]]:
        if not await Match.objects.filter(match_id=p["match_id"]).aexists():
            msg = f"Match {p['match_id']} not found."
            raise Http404(msg)
        pms = [pm async for pm in PlayerMatch.objects.filter(match_id=p["match_id"]).select_related("hero", "player")]
        return PlayerMatchSerializer.serialize_many(pms, include_hero=p["include_hero"])


class MatchComprehensiveView(BaseAppView):
    """GET /api/v1/matches/{match_id}/full/"""

    CACHE_TTL = TIMEOUTS.get("match_comprehensive", 1800)

    def _get_params(self, request: HttpRequest, **kwargs) -> dict[str, Any]:
        return {"match_id": kwargs["match_id"]}

    async def _produce_payload(self, p: dict[str, Any]) -> dict[str, Any]:
        match_task = MatchSerializer.base_queryset(stats=True, patch=True).aget(match_id=p["match_id"])
        player_task = PlayerMatch.objects.filter(match_id=p["match_id"]).select_related("hero", "player").all()
        try:
            match_obj, pms = await asyncio.gather(match_task, player_task)
        except Match.DoesNotExist as e:
            msg = f"Match {p['match_id']} not found."
            raise Http404(msg) from e

        winside = {1: "Radiant", 0: "Dire"}.get(match_obj.winner, "Unknown")
        return {
            "match": MatchSerializer.serialize_match(match_obj, include_patch=True, include_stats=True),
            "scoreboard": PlayerMatchSerializer.serialize_many(pms),
            "insights": {
                "kill_diff": abs(match_obj.radiant_score - match_obj.dire_score),
                "winner": winside,
                "duration_minutes": round(match_obj.duration / 60, 1),
            },
            "updated_at": timezone.now().isoformat(timespec="seconds"),
        }


class LongestMatchesView(BaseAppView):
    """GET /api/v1/matches/longest/"""

    CACHE_TTL = TIMEOUTS["longest_matches"]

    def _get_params(self, request: HttpRequest, **kwargs) -> dict[str, Any]:
        return {
            "limit": self.get_int_param(request, "limit", default=50, max_val=200),
            "after_ts": self.get_int_param(request, "after", default=0, min_val=0),
        }

    async def _produce_payload(self, p: dict[str, Any]) -> list[dict[str, Any]]:
        qs = Match.objects.filter(start_time__gte=p["after_ts"]).order_by("-duration").select_related("stats")
        matches = [m async for m in qs[: p["limit"]]]
        return MatchSerializer.serialize_matches(matches, include_stats=True)
