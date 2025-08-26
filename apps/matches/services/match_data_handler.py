# ===============================================================================
# apps/matches/services/match_data_handler.py
# ===============================================================================
from __future__ import annotations

import asyncio
from collections import Counter
from itertools import islice
from typing import TYPE_CHECKING, Any, Final

import structlog
from asgiref.sync import sync_to_async
from django.db import transaction
from django.db.utils import IntegrityError

from apps.core.datatype import UpsertResult, new_upsert_result
from apps.leagues.models import League
from apps.matches.models import Match, MatchStats
from apps.matches.schemas.match_row import MatchRow
from apps.matches.services.picks_bans_handler import PickBanDataHandler
from apps.matches.services.player_match_data_handler import PlayerMatchDataHandler
from apps.teams.models import Team, TeamMatch

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable, Mapping, Sequence

# -------------------------------------------------------------------------------
# Logger
# -------------------------------------------------------------------------------
log: Final = structlog.get_logger(__name__).bind(handler="MatchDataHandler")


class MatchDataHandler:
    """
    Orchestrates the parsing and database upserting of full match data
    (Match, MatchStats, TeamMatch, plus delegated Player/PickBan tables).
    """

    # ---------------------------------------------------------------------------
    # Construction
    # ---------------------------------------------------------------------------
    def __init__(self) -> None:
        self._pick_ban_handler = PickBanDataHandler()
        self._player_match_handler = PlayerMatchDataHandler()

    # ---------------------------------------------------------------------------
    # Public async entry-point
    # ---------------------------------------------------------------------------
    async def upsert_async(
        self,
        rows: Iterable[Mapping[str, Any]],
        *,
        bulk_size: int = 200,
        chunk_size: int = 1_000,
    ) -> UpsertResult:
        """
        Asynchronously parse, validate, and upsert a stream of raw match data.

        The work is done in chunks so we never accumulate unbounded memory.
        """
        total_result = new_upsert_result()
        rows_iter = iter(rows)

        while chunk := list(islice(rows_iter, chunk_size)):
            # 1) Parse & validate -------------------------------------------------
            parsed_rows: list[MatchRow] = [p for raw in chunk if (p := MatchRow.parse(raw))]

            total_result["skipped"] += len(chunk) - len(parsed_rows)
            if not parsed_rows:
                continue

            # 2) Core upsert (runs in sync thread) -------------------------------
            created = await sync_to_async(
                self._bulk_upsert_sync,
                thread_sensitive=True,
            )(parsed_rows, bulk_size)

            total_result["created"] += created
            total_result["updated"] += len(parsed_rows) - created

            # 3) Delegate to sub-handlers (players, picks/bans) ------------------
            sub_tasks = []
            for row in parsed_rows:
                if row.players:
                    sub_tasks.append(
                        self._player_match_handler.upsert_async(row.match_id, row.players),
                    )
                if row.picks_bans:
                    sub_tasks.append(
                        self._pick_ban_handler.upsert_async(row.match_id, row.picks_bans),
                    )

            if sub_tasks:
                sub_results = await asyncio.gather(*sub_tasks)

                aggregated: Counter[str] = Counter()
                for r in sub_results:
                    aggregated.update(r)

                total_result["created"] += aggregated.get("created", 0)
                total_result["updated"] += aggregated.get("updated", 0)
                total_result["skipped"] += aggregated.get("skipped", 0)

        return total_result

    # ---------------------------------------------------------------------------
    # Internal helpers (sync)
    # ---------------------------------------------------------------------------
    @staticmethod
    def _ensure_dependencies_exist_sync(rows: Sequence[MatchRow]) -> None:
        """
        Ensure FK targets (Team, League) exist before inserting Match / Stats /
        TeamMatch rows.  Empty shell rows are fine; later ETL passes can enrich
        them.
        """
        team_ids = {int(r.radiant_team_id) for r in rows if r.radiant_team_id} | {
            int(r.dire_team_id) for r in rows if r.dire_team_id
        }
        league_ids = {int(r.league_id) for r in rows if r.league_id}

        if team_ids:
            Team.objects.bulk_create(
                [Team(team_id=tid) for tid in team_ids],
                ignore_conflicts=True,
            )
        if league_ids:
            League.objects.bulk_create(
                [League(league_id=lid) for lid in league_ids],
                ignore_conflicts=True,
            )

    # ---------------------------------------------------------------------------
    # Core sync upsert
    # ---------------------------------------------------------------------------
    def _bulk_upsert_sync(self, parsed_rows: Sequence[MatchRow], bulk_size: int) -> int:
        match_ids = [row.match_id for row in parsed_rows]
        existing_count = Match.objects.filter(match_id__in=match_ids).count()

        try:
            with transaction.atomic():
                self._ensure_dependencies_exist_sync(parsed_rows)

                match_objs = [Match(match_id=row.match_id, **row.match_fields()) for row in parsed_rows]
                stats_objs = [MatchStats(match_id=row.match_id, **row.stats_fields()) for row in parsed_rows]

                Match.objects.bulk_create(
                    match_objs,
                    update_conflicts=True,
                    unique_fields=["match_id"],
                    update_fields=list(parsed_rows[0].match_fields().keys()),
                    batch_size=bulk_size,
                )

                MatchStats.objects.bulk_create(
                    stats_objs,
                    update_conflicts=True,
                    unique_fields=["match_id"],
                    update_fields=list(parsed_rows[0].stats_fields().keys()),
                    batch_size=bulk_size,
                )

                # TeamMatch deduplication
                seen_pairs = set()
                team_match_objs = []
                for row in parsed_rows:
                    if row.radiant_team_id and (pair := (row.radiant_team_id, row.match_id)) not in seen_pairs:
                        team_match_objs.append(TeamMatch(team_id=row.radiant_team_id, match_id=row.match_id, radiant=True))
                        seen_pairs.add(pair)
                    if row.dire_team_id and (pair := (row.dire_team_id, row.match_id)) not in seen_pairs:
                        team_match_objs.append(TeamMatch(team_id=row.dire_team_id, match_id=row.match_id, radiant=False))
                        seen_pairs.add(pair)

                if team_match_objs:
                    TeamMatch.objects.bulk_create(team_match_objs, ignore_conflicts=True, batch_size=bulk_size)

            return len(parsed_rows) - existing_count
        except IntegrityError as e:
            log.exception("Integrity error during match upsert.", exc_info=e)
            raise
        except Exception as e:
            log.critical("Unexpected error during match upsert.", exc_info=e)
            raise
