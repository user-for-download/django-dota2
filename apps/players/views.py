# apps/players/views.py (Refactored)
# ======================================================================
"""Asynchronous API views for the 'players' application, refactored for clarity."""

from __future__ import annotations

from typing import Any

import structlog
from django.http import Http404, HttpRequest

from common.views_utils import BaseAppView

# --- The new, powerful base class ---
from . import conf
from .models import Player, PlayerRating, RankTier
from .serializers import PlayerSerializer

log = structlog.get_logger(__name__).bind(component="PlayersViews")


class PlayerListView(BaseAppView):
    """GET /api/v1/players/ – recent-activity list."""

    CACHE_TTL = conf.PLAYER_TIMEOUTS.get("list", 300)

    def _get_params(self, request: HttpRequest, **kwargs) -> dict[str, Any]:
        page = self.get_page(request)
        return {"page_num": page.number, "page_size": page.size, "offset": page.offset}

    async def _produce_payload(self, p: dict[str, Any]) -> dict[str, Any]:
        qs = Player.objects.values("account_id", "personaname", "avatar", "last_match_time").order_by(
            "-last_match_time",
        )
        total = await qs.acount()
        players = [player async for player in qs[p["offset"] : p["offset"] + p["page_size"]]]
        return {
            "count": total,
            "page": p["page_num"],
            "page_size": p["page_size"],
            "total_pages": -(-total // p["page_size"]) if p["page_size"] else 0,
            "data": PlayerSerializer.serialize_list_items(players),
        }


class ProPlayerListView(BaseAppView):
    """GET /api/v1/players/pro/ – all professional players."""

    CACHE_TTL = conf.PLAYER_TIMEOUTS.get("pro_list", 600)

    def _get_params(self, request: HttpRequest, **kwargs) -> dict[str, Any]:
        page = self.get_page(request)
        return {"page_num": page.number, "page_size": page.size, "offset": page.offset}

    async def _produce_payload(self, p: dict[str, Any]) -> dict[str, Any]:
        qs = (
            Player.objects.filter(notable_profile__is_pro=True)
            .select_related("notable_profile__team")
            .order_by("notable_profile__name")
        )
        total = await qs.acount()
        players = [player async for player in qs[p["offset"] : p["offset"] + p["page_size"]]]
        return {
            "count": total,
            "page": p["page_num"],
            "page_size": p["page_size"],
            "total_pages": -(-total // p["page_size"]) if p["page_size"] else 0,
            "data": [PlayerSerializer.serialize_pro_list_item(player) for player in players],
        }


class PlayerDetailView(BaseAppView):
    """GET /api/v1/players/{account_id}/ – detailed info."""

    CACHE_TTL = conf.PLAYER_TIMEOUTS.get("detail", 300)

    def _get_params(self, request: HttpRequest, **kwargs) -> dict[str, Any]:
        return {
            "account_id": kwargs["account_id"],
            "inc_notable": self.get_bool_param(request, "include_notable_profile"),
            "inc_ranks": self.get_bool_param(request, "include_ranks"),
        }

    async def _produce_payload(self, p: dict[str, Any]) -> dict[str, Any]:
        qs = Player.objects.filter(account_id=p["account_id"])
        selects = []
        if p["inc_notable"]:
            selects.append("notable_profile__team")
        if p["inc_ranks"]:
            qs = qs.with_ranks()
        if selects:
            qs = qs.select_related(*selects).only("account_id", "personaname", "avatar", "profileurl", "plus", "last_login", "loccountrycode", "last_match_time", *[f"{s}__rating" for s in selects if "__" not in s])

        try:
            player = await qs.aget()
        except Player.DoesNotExist as e:
            msg = f"Player {p['account_id']} not found."
            raise Http404(msg) from e

        return PlayerSerializer.serialize_player_detail(
            player,
            include_notable_profile=p["inc_notable"],
            include_ranks=p["inc_ranks"],
        )


class PlayerRatingHistoryView(BaseAppView):
    """GET /api/v1/players/{account_id}/ratings/ – historical MMR."""

    CACHE_TTL = conf.PLAYER_TIMEOUTS.get("ratings", 300)

    def _get_params(self, request: HttpRequest, **kwargs) -> dict[str, Any]:
        page = self.get_page(request)
        return {
            "account_id": kwargs["account_id"],
            "page_num": page.number,
            "page_size": page.size,
            "offset": page.offset,
        }

    async def _produce_payload(self, p: dict[str, Any]) -> list[dict[str, Any]]:
        if not await Player.objects.filter(account_id=p["account_id"]).aexists():
            msg = f"Player {p['account_id']} not found."
            raise Http404(msg)

        qs = PlayerRating.objects.filter(player_id=p["account_id"])
        ratings = [r async for r in qs[p["offset"] : p["offset"] + p["page_size"]]]
        return [PlayerSerializer.serialize_rating_history(r) for r in ratings]


class PlayerRankTierView(BaseAppView):
    """GET /api/v1/players/{account_id}/rank/ – current rank tier."""

    CACHE_TTL = conf.PLAYER_TIMEOUTS.get("rank", 300)

    def _get_params(self, request: HttpRequest, **kwargs) -> dict[str, Any]:
        return {"account_id": kwargs["account_id"]}

    async def _produce_payload(self, p: dict[str, Any]) -> dict[str, Any]:
        try:
            rank = await RankTier.objects.aget(player_id=p["account_id"])
        except RankTier.DoesNotExist as e:
            msg = f"Rank tier for player {p['account_id']} not found."
            raise Http404(msg) from e
        return PlayerSerializer.serialize_rank_tier(rank)
