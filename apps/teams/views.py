# apps/teams/views.py (Refactored)
# ======================================================================
"""Asynchronous API views for the 'teams' application, refactored for clarity and maintainability."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

import structlog
from django.http import Http404, HttpRequest

# --- Refactored Base Class ---
from apps.teams import conf
from apps.teams.models import Team
from apps.teams.serializers import TeamSerializer
from common.views_utils import BaseAppView

log = structlog.get_logger(__name__).bind(component="TeamViews")


def _safe_decimal_to_int(raw: str | None, scale: int = 100) -> int:
    try:
        return int(Decimal(raw or "0") * scale)
    except (InvalidOperation, ValueError):
        return 0


# --- /teams/ ---
class TeamListView(BaseAppView):
    """GET /api/v1/teams/ – list teams with optional rating / player filters."""

    CACHE_TTL = conf.TIMEOUTS.get("team_list", 300)

    def _get_params(self, request: HttpRequest, **kwargs) -> dict[str, Any]:
        page = self.get_page(request)
        return {
            "page_num": page.number,
            "page_size": page.size,
            "offset": page.offset,
            "search_q": request.GET.get("search", "").strip(),
            "inc_rating": self.get_bool_param(request, "include_rating"),
            "inc_players": self.get_bool_param(request, "include_players"),
            "is_active": request.GET.get("active"),
            "min_rating": _safe_decimal_to_int(request.GET.get("min_rating")),
        }

    async def _produce_payload(self, p: dict[str, Any]) -> dict[str, Any]:
        qs = Team.objects.all()
        if p["inc_rating"] or p["min_rating"]:
            qs = qs.with_rating()
        if p["inc_players"]:
            qs = qs.with_player_count()
        if p["search_q"]:
            qs = qs.search_by_name(p["search_q"])
        if p["is_active"] in ("true", "false"):
            qs = qs.filter(is_active=(p["is_active"] == "true"))
        if p["min_rating"]:
            qs = qs.filter(rating__rating__gte=p["min_rating"])

        total = await qs.acount()
        teams = [t async for t in qs[p["offset"] : p["offset"] + p["page_size"]]]

        return {
            "count": total,
            "page": p["page_num"],
            "page_size": p["page_size"],
            "total_pages": -(-total // p["page_size"]) if p["page_size"] else 0,
            "data": TeamSerializer.serialize_teams(
                teams,
                include_rating=p["inc_rating"],
                include_players=p["inc_players"],
            ),
        }


# --- /teams/<id>/ ---
class TeamDetailView(BaseAppView):
    """GET /api/v1/teams/{team_id}/ – single-team detail."""

    CACHE_TTL = conf.TIMEOUTS.get("team_detail", 3600)

    def _get_params(self, request: HttpRequest, **kwargs) -> dict[str, Any]:
        return {
            "team_id": kwargs["team_id"],
            "inc_rating": self.get_bool_param(request, "include_rating"),
            "inc_players": self.get_bool_param(request, "include_players"),
        }

    async def _produce_payload(self, p: dict[str, Any]) -> dict[str, Any]:
        qs = Team.objects.filter(team_id=p["team_id"])
        if p["inc_rating"]:
            qs = qs.with_rating()
        if p["inc_players"]:
            qs = qs.with_player_count()
        try:
            team = await qs.aget()
        except Team.DoesNotExist as e:
            msg = f"Team {p['team_id']} not found."
            raise Http404(msg) from e
        return TeamSerializer.serialize_team(team, include_rating=p["inc_rating"], include_players=p["inc_players"])


# --- /teams/leaderboard/ ---
class LeaderboardView(BaseAppView):
    """GET /api/v1/teams/leaderboard/"""

    CACHE_TTL = conf.TIMEOUTS.get("leaderboard", 3600)

    def _get_params(self, request: HttpRequest, **kwargs) -> dict[str, Any]:
        return {
            "limit": self.get_int_param(
                request,
                "limit",
                default=conf.DEFAULT_LEADERBOARD_LIMIT,
                max_val=conf.MAX_LEADERBOARD_LIMIT,
            ),
            "min_rating": _safe_decimal_to_int(request.GET.get("min_rating")),
        }

    async def _produce_payload(self, p: dict[str, Any]) -> dict[str, Any]:
        qs = Team.objects.leaderboard(min_rating_int=p["min_rating"], limit=p["limit"])
        teams = [t async for t in qs]
        return TeamSerializer.serialize_leaderboard(teams)
