# apps/leagues/views.py (Refactored)
# ======================================================================
"""Async-first API views for browsing and retrieving league information."""

from __future__ import annotations

from typing import Any

import structlog
from django.http import Http404, HttpRequest

from common.views_utils import BaseAppView

from .conf import LEAGUE_TIMEOUTS
from .models import League
from .serializers import LeagueSerializer

log = structlog.get_logger(__name__).bind(component="LeagueViews")


class LeagueListView(BaseAppView):
    """GET /api/v1/leagues/ – paginated & filterable."""

    CACHE_TTL = LEAGUE_TIMEOUTS.get("list", 300)

    def _get_params(self, request: HttpRequest, **kwargs) -> dict[str, Any]:
        page = self.get_page(request)
        return {
            "page_num": page.number,
            "page_size": page.size,
            "offset": page.offset,
            "search_q": request.GET.get("search", "").strip(),
            "only_pro": self.get_bool_param(request, "pro"),
            "only_active": self.get_bool_param(request, "active"),
        }

    async def _produce_payload(self, p: dict[str, Any]) -> dict[str, Any]:
        qs = League.objects.all()
        if p["only_pro"]:
            qs = qs.professional()
        if p["only_active"]:
            qs = qs.active()
        if p["search_q"]:
            qs = qs.filter(name__icontains=p["search_q"])

        # Only fetch fields we need
        qs = qs.values("league_id", "name", "tier", "banner").order_by("-league_id")

        total = await qs.acount()
        page = qs[p["offset"] : p["offset"] + p["page_size"]]
        rows = [row async for row in page]

        # Manual serialization
        data = []
        for row in rows:
            data.append({
                "league_id": row["league_id"],
                "name": row["name"],
                "tier": row["tier"],
                "banner_url": row["banner"],
                "is_professional": row["tier"] in {"premium", "professional"},
            })

        return {
            "count": total,
            "page": p["page_num"],
            "page_size": p["page_size"],
            "total_pages": -(-total // p["page_size"]),
            "data": data,
        }


class LeagueDetailView(BaseAppView):
    """GET /api/v1/leagues/{league_id}/ – one league, with counts."""

    CACHE_TTL = LEAGUE_TIMEOUTS.get("detail", 3600)

    def _get_params(self, request: HttpRequest, **kwargs) -> dict[str, Any]:
        return {"league_id": kwargs["league_id"]}

    async def _produce_payload(self, p: dict[str, Any]) -> dict[str, Any]:
        try:
            league = await League.objects.with_match_count().with_team_count().aget(pk=p["league_id"])
        except League.DoesNotExist as e:
            msg = f"League {p['league_id']} not found."
            raise Http404(msg) from e
        return LeagueSerializer.serialize_league(league)
