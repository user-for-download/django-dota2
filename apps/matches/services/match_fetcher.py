"""
Concrete implementation for fetching & persisting match data.

Unlike the other domains, matches are pulled from the local DB through an
Explorer-style SQL endpoint (handled by DotaDataHandler).  No static fallback.
Compatible with Python 3.12 / Django 5.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

import structlog

from apps.core.services.base_fetcher import BaseFetcher
from apps.core.services.dota_data_handler import DotaDataHandler
from apps.matches.conf import MatchFetcherConfig
from apps.matches.services.match_data_handler import MatchDataHandler
from apps.matches.services.queries import build_full_matches_data_query

if TYPE_CHECKING:
    from collections.abc import Mapping

    from apps.core.conf import SQLGen

log = structlog.get_logger(__name__).bind(fetcher="MatchFetcher")


class MatchFetcher(BaseFetcher[MatchFetcherConfig, MatchDataHandler]):
    # ───────────────────────── default wiring ──────────────────────────
    def _default_config(self) -> MatchFetcherConfig:
        cfg = MatchFetcherConfig()
        cfg.check()
        return cfg

    def _default_handler(self) -> MatchDataHandler:
        return MatchDataHandler()

    def _fetcher_type(self) -> Literal["matches"]:
        return "matches"

    def _fallback_url(self) -> str | None:  # matches are dynamic → no fallback
        return None

    def _count_query(self) -> str:
        return "SELECT COUNT(*) FROM matches"

    # ───────────────────────── data fetching ──────────────────────────
    async def _fetch_primary_source(self) -> list[list[dict[str, Any]]]:
        """
        Pull match rows from the local DB through the Explorer API.
        """
        if getattr(self.cfg, "skip_fetching", False):
            log.info("skip primary fetch (cfg.skip_fetching)")
            return []

        if not self.cfg.match_ids:
            log.info("cfg.match_ids empty → nothing to fetch")
            return []

        query_generators = self._get_query_generators()
        async with DotaDataHandler(query_generators=query_generators, session=self._session) as handler:
            return await handler.fetch_and_chunk("matches", chunk_size=self.cfg.limit)

    async def _validate_specific(self) -> None:  # no extra validation needed
        ...

    # ───────────────────────── helpers ──────────────────────────
    def _get_query_generators(self) -> Mapping[str, SQLGen]:
        """
        Supply SQL generator(s) to DotaDataHandler.  Captures current cfg.
        """

        return {
            "matches": lambda: build_full_matches_data_query(
                match_ids=self.cfg.match_ids,
            ),
        }
