"""
Concrete implementation for fetching and persisting professional-player data.
Compatible with Python 3.12 / Django 5 and the updated BaseFetcher.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

import structlog

from apps.core.services.base_fetcher import BaseFetcher
from apps.core.services.dota_data_handler import DotaDataHandler
from apps.players.conf import PRO_PLAYERS_FALLBACK_URL, PlayerFetcherConfig
from apps.players.services.player_data_handler import PlayerDataHandler
from apps.players.services.queries import build_players_with_matches_ids_sql

if TYPE_CHECKING:
    from collections.abc import Mapping

    from apps.core.conf import SQLGen

log = structlog.get_logger(__name__).bind(fetcher="PlayerFetcher")


class PlayerFetcher(BaseFetcher[PlayerFetcherConfig, PlayerDataHandler]):
    # ───────────────────────── default wiring ──────────────────────────
    def _default_config(self) -> PlayerFetcherConfig:
        cfg = PlayerFetcherConfig()
        cfg.check()
        return cfg

    def _default_handler(self) -> PlayerDataHandler:
        return PlayerDataHandler()

    def _fetcher_type(self) -> Literal["players"]:
        return "players"

    def _fallback_url(self) -> str:
        return PRO_PLAYERS_FALLBACK_URL

    def _count_query(self) -> str:
        return "SELECT COUNT(*) FROM players"

    # ───────────────────────── data fetching ──────────────────────────
    async def _fetch_primary_source(self) -> list[list[dict[str, Any]]]:
        """
        Pull pro-player rows from the Explorer API, validate, chunk.
        Skips when cfg.skip_matches is true (e.g. metrics-only run).
        """
        if self.cfg.skip_matches:
            log.info("skip primary fetch (cfg.skip_matches)")
            return []

        query_generators = self._get_query_generators()
        async with DotaDataHandler(query_generators=query_generators, session=self._session) as handler:
            return await handler.fetch_and_chunk("players", chunk_size=1_000)

    async def _validate_specific(self) -> None:  # nothing extra for players
        ...

    # ───────────────────────── helpers ──────────────────────────
    def _get_query_generators(self) -> Mapping[str, SQLGen]:
        """
        Return mapping understood by DotaDataHandler.  You can add additional
        keys later (e.g. 'players_detailed') without touching the handler.
        """
        return {
            "players": lambda: build_players_with_matches_ids_sql(
                limit=self.cfg.limit,
                days=30,  # domain-specific param (example)
            ),
        }
