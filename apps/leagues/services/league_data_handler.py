# /home/ubuntu/dota/apps/leagues/services/league_data_handler.py
# ================================================================================
"""
LeagueDataHandler – Parses raw league data and performs efficient bulk upserts
into the League model.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import islice
from typing import TYPE_CHECKING, Any, ClassVar, Self

import structlog
from asgiref.sync import sync_to_async
from django.db import IntegrityError, transaction

from apps.core.datatype import UpsertResult, new_upsert_result
from apps.leagues.models import League

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

log = structlog.get_logger(__name__).bind(handler="LeagueDataHandler")


@dataclass(slots=True, frozen=True)
class _ValidatedLeagueRow:
    """A validated, internal Data Transfer Object for a single league row."""

    leagueid: int
    name: str | None = None
    tier: str | None = None
    banner: str | None = None
    ticket: str | None = None

    LEAGUE_FIELDS: ClassVar[tuple[str, ...]] = ("name", "tier", "banner", "ticket")

    @classmethod
    def parse(cls, src: dict[str, Any]) -> Self | None:
        """Safely parses a raw dictionary, returning a validated row or None."""
        try:
            league_id = int(src["leagueid"])
        except (KeyError, TypeError, ValueError):
            log.warning("Skipping row: missing or invalid 'leagueid'", raw_data=src)
            return None
        return cls(
            leagueid=league_id,
            name=src.get("name"),
            tier=src.get("tier"),
            banner=src.get("banner"),
            ticket=src.get("ticket"),
        )

    def league_kwargs(self) -> dict[str, Any]:
        """Returns a dict of non-None values for creating/updating a League object."""
        return {k: v for k in self.LEAGUE_FIELDS if (v := getattr(self, k)) is not None}


class LeagueDataHandler:
    """
    Converts raw API rows into League model instances and performs bulk upserts
    asynchronously by offloading sync ORM calls to a thread.
    """

    async def upsert_async(
        self,
        rows: Iterable[dict[str, Any]],
        *,
        bulk_size: int = 1_000,
        chunk_size: int = 5_000,
    ) -> UpsertResult:
        """Validates, parses, and chunks data for efficient database upserting."""
        rows_iter = iter(rows)
        total_result = new_upsert_result()

        while chunk := list(islice(rows_iter, chunk_size)):
            parsed_rows = [p for r in chunk if (p := _ValidatedLeagueRow.parse(r))]
            total_result["skipped"] += len(chunk) - len(parsed_rows)
            if not parsed_rows:
                continue

            # --- THE FIX ---
            # Use thread_sensitive=True because the sync code uses transactions.
            created_count = await sync_to_async(
                self._bulk_upsert_sync,
                thread_sensitive=True,
            )(parsed_rows, bulk_size)
            # --- END FIX ---

            total_result["created"] += created_count
            total_result["updated"] += len(parsed_rows) - created_count

        return total_result

    @staticmethod
    def _bulk_upsert_sync(parsed_rows: Sequence[_ValidatedLeagueRow], bulk_size: int) -> int:
        league_ids = [row.leagueid for row in parsed_rows]
        existing_count = League.objects.filter(league_id__in=league_ids).count()

        league_objs = [
            League(league_id=row.leagueid, **row.league_kwargs())
            for row in parsed_rows
        ]

        try:
            with transaction.atomic():
                League.objects.bulk_create(
                    league_objs,
                    update_conflicts=True,
                    unique_fields=["league_id"],
                    update_fields=_ValidatedLeagueRow.LEAGUE_FIELDS,
                    batch_size=bulk_size,
                )
            return len(parsed_rows) - existing_count
        except IntegrityError as e:
            log.exception("league_bulk_upsert_integrity_error", exc_info=e)
            return 0
        except Exception as e:
            log.exception("league_bulk_upsert_unexpected_error", exc_info=e)
            raise
