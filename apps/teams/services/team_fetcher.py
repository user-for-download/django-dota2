"""
Concrete TeamFetcher built on BaseFetcher.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

import structlog

from apps.core.services.base_fetcher import BaseFetcher
from apps.core.services.dota_data_handler import DotaDataHandler
from apps.teams.conf import TEAMS_FALLBACK_URL, TeamFetcherConfig
from apps.teams.services.queries import build_teams_with_matches_ids_sql
from apps.teams.services.team_data_handler import TeamDataHandler

if TYPE_CHECKING:
    from collections.abc import Mapping

    from apps.core.conf import SQLGen

log = structlog.get_logger(__name__).bind(fetcher="TeamFetcher")


class TeamFetcher(BaseFetcher[TeamFetcherConfig, TeamDataHandler]):
    # ---------------------------------------------------------------- default impl
    def _default_config(self) -> TeamFetcherConfig:
        cfg = TeamFetcherConfig()
        cfg.check()
        return cfg

    def _default_handler(self) -> TeamDataHandler:
        return TeamDataHandler()

    def _fetcher_type(self) -> Literal["teams"]:
        return "teams"

    def _fallback_url(self) -> str:
        return TEAMS_FALLBACK_URL

    def _count_query(self) -> str:
        return "SELECT COUNT(*) FROM teams"

    # ---------------------------------------------------------------- fetching
    async def _fetch_primary_source(self) -> list[list[dict[str, Any]]]:
        if self.cfg.skip_matches:
            log.info("skip primary fetch (cfg.skip_matches)")
            return []

        query_generators = self._get_query_generators()
        async with DotaDataHandler(query_generators=query_generators, session=self._session) as handler:
            return await handler.fetch_and_chunk("teams", chunk_size=1_000)

    # ---------------------------------------------------------------- helpers
    def _get_query_generators(self) -> Mapping[str, SQLGen]:
        return {
            "teams": lambda: build_teams_with_matches_ids_sql(
                limit=self.cfg.limit,
                min_rating=self.cfg.min_rating,
            ),
        }
