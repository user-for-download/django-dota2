"""
Concrete implementation for fetching & persisting league data.
Ready for Python 3.12 / Django 5 and the revamped BaseFetcher stack.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

import structlog

from apps.core.services.base_fetcher import BaseFetcher
from apps.core.services.dota_data_handler import DotaDataHandler
from apps.leagues.conf import LEAGUES_FALLBACK_URL, LeagueFetcherConfig
from apps.leagues.services.league_data_handler import LeagueDataHandler
from apps.leagues.services.queries import build_leagues_with_matches_ids_sql

if TYPE_CHECKING:
    from collections.abc import Mapping

    from apps.core.conf import SQLGen

log = structlog.get_logger(__name__).bind(fetcher="LeagueFetcher")


class LeagueFetcher(BaseFetcher[LeagueFetcherConfig, LeagueDataHandler]):
    # ───────────────────────── default wiring ──────────────────────────
    def _default_config(self) -> LeagueFetcherConfig:
        cfg = LeagueFetcherConfig()
        cfg.check()
        return cfg

    def _default_handler(self) -> LeagueDataHandler:
        return LeagueDataHandler()

    def _fetcher_type(self) -> Literal["leagues"]:
        return "leagues"

    def _fallback_url(self) -> str:
        return LEAGUES_FALLBACK_URL

    def _count_query(self) -> str:
        return "SELECT COUNT(*) FROM leagues"

    # ───────────────────────── data fetching ──────────────────────────
    async def _fetch_primary_source(self) -> list[list[dict[str, Any]]]:
        """
        Pull league rows from the Explorer API, validate & chunk.
        """
        if self.cfg.skip_matches:
            log.info("skip primary fetch (cfg.skip_matches)")
            return []

        query_gens = self._get_query_generators()
        async with DotaDataHandler(query_generators=query_gens, session=self._session) as handler:
            return await handler.fetch_and_chunk("leagues", chunk_size=1_000)

    async def _validate_specific(self) -> None:  # nothing extra
        ...

    # ───────────────────────── helpers ──────────────────────────
    def _get_query_generators(self) -> Mapping[str, SQLGen]:
        """
        Map dtype -> SQL generator callable understood by DotaDataHandler.
        """
        return {
            "leagues": lambda: build_leagues_with_matches_ids_sql(
                limit=self.cfg.limit,
                patch=True,  # domain-specific flag
            ),
        }
