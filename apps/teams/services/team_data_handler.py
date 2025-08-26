# /home/ubuntu/dota/apps/teams/services/team_data_handler.py
# ================================================================================
"""
TeamDataHandler – Parses raw team data and performs efficient bulk upserts
into the Team and TeamRating models.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import islice
from typing import TYPE_CHECKING, Any, ClassVar

import structlog
from asgiref.sync import sync_to_async
from django.db import IntegrityError, transaction

from apps.core.datatype import (
    UpsertResult,  # Import the factory
    new_upsert_result,  # Import the factory
)
from apps.teams.models import Team, TeamRating

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence
    from typing import Self

log = structlog.get_logger(__name__).bind(handler="TeamDataHandler")


@dataclass(slots=True, frozen=True)
class _ValidatedTeamRow:
    """A validated, internal representation of a single team data row from an API."""

    team_id: int
    name: str | None = None
    tag: str | None = None
    logo_url: str | None = None
    rating: int | None = None
    wins: int | None = None
    losses: int | None = None
    last_match_time: int | None = None

    TEAM_FIELDS: ClassVar[tuple[str, ...]] = ("name", "tag", "logo_url")
    RATING_FIELDS: ClassVar[tuple[str, ...]] = ("rating", "wins", "losses", "last_match_time")

    @classmethod
    def parse(cls, src: dict[str, Any]) -> Self | None:
        try:
            team_id = int(src["team_id"])
        except (KeyError, TypeError, ValueError):
            log.warning("Skipping row: invalid or missing team_id.", raw_row=src)
            return None
        rating_raw = src.get("rating")
        rating_int = int(rating_raw * 100) if isinstance(rating_raw, int | float) else None
        return cls(
            team_id=team_id,
            name=src.get("name") or src.get("team_name"),
            tag=src.get("tag"),
            logo_url=src.get("logo_url"),
            rating=rating_int,
            wins=src.get("wins"),
            losses=src.get("losses"),
            last_match_time=src.get("last_match_time"),
        )

    def team_kwargs(self) -> dict[str, Any]:
        return {k: v for k in self.TEAM_FIELDS if (v := getattr(self, k)) is not None}

    def rating_kwargs(self) -> dict[str, Any]:
        return {k: v for k in self.RATING_FIELDS if (v := getattr(self, k)) is not None}


class TeamDataHandler:
    """Converts API rows into model instances and performs bulk upserts asynchronously."""

    async def upsert_async(
        self,
        rows: Iterable[dict[str, Any]],
        *,
        bulk_size: int = 1_000,
        chunk_size: int = 5_000,
    ) -> UpsertResult:
        """Asynchronously validates and upserts team data in chunks."""
        total_result = new_upsert_result()
        rows_iter = iter(rows)

        while chunk := list(islice(rows_iter, chunk_size)):
            parsed_rows = [p for r in chunk if (p := _ValidatedTeamRow.parse(r))]
            total_result["skipped"] += len(chunk) - len(parsed_rows)
            if not parsed_rows:
                continue

            # --- THE FIX ---
            # Use thread_sensitive=True because the sync code uses transactions.
            created_in_batch = await sync_to_async(
                self._bulk_upsert_sync,
                thread_sensitive=True,
            )(parsed_rows, bulk_size)
            # --- END FIX ---

            total_result["created"] += created_in_batch
            total_result["updated"] += len(parsed_rows) - created_in_batch

        return total_result

    @staticmethod
    def _bulk_upsert_sync(parsed_rows: Sequence[_ValidatedTeamRow], bulk_size: int) -> int:
        team_ids = [row.team_id for row in parsed_rows]
        existing_count = Team.objects.filter(team_id__in=team_ids).count()

        team_objs = [
            Team(team_id=row.team_id, **row.team_kwargs())
            for row in parsed_rows
        ]
        rating_objs = [
            TeamRating(team_id=row.team_id, **row.rating_kwargs())
            for row in parsed_rows if row.rating_kwargs()
        ]

        try:
            with transaction.atomic():
                Team.objects.bulk_create(
                    team_objs,
                    update_conflicts=True,
                    unique_fields=["team_id"],
                    update_fields=_ValidatedTeamRow.TEAM_FIELDS,
                    batch_size=bulk_size,
                )
                if rating_objs:
                    TeamRating.objects.bulk_create(
                        rating_objs,
                        update_conflicts=True,
                        unique_fields=["team_id"],
                        update_fields=_ValidatedTeamRow.RATING_FIELDS,
                        batch_size=bulk_size,
                    )
            return len(parsed_rows) - existing_count
        except IntegrityError as e:
            log.exception("team_upsert_integrity_error", exc_info=e)
            return 0
        except Exception as e:
            log.critical("team_upsert_unexpected_error", exc_info=e)
            raise
