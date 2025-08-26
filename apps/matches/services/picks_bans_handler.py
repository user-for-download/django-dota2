# /home/ubuntu/dota/apps/matches/services/picks_bans_handler.py
# ================================================================================
"""
Handles the efficient bulk creation of immutable pick/ban records for a given match.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

import structlog
from asgiref.sync import sync_to_async
from django.db import transaction
from django.db.utils import IntegrityError

from apps.core.datatype import UpsertResult
from apps.matches.models import PickBan
from apps.matches.schemas.pickban_row import PickBanRow

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

log: Final = structlog.get_logger(__name__).bind(handler="PickBanDataHandler")


class PickBanDataHandler:
    """
    Parses and persists the draft (picks and bans) for a single match.
    Since draft data is immutable, this handler only performs creates, not updates.
    """

    async def upsert_async(
        self,
        match_id: int,
        rows: Iterable[dict[str, Any]],
        *,
        bulk_size: int = 50,
    ) -> UpsertResult:
        """Asynchronously creates pick/ban records from a list of raw data rows."""
        row_list = list(rows)
        if not row_list:
            return UpsertResult(created=0, updated=0, skipped=0)

        parsed_rows = [p for r in row_list if (p := PickBanRow.parse(r, match_id))]
        skipped = len(row_list) - len(parsed_rows)
        created = 0

        if parsed_rows:
            # Offload the synchronous database operation to a thread.
            created = await sync_to_async(self._bulk_create_sync, thread_sensitive=True)(parsed_rows, bulk_size)

        # This is a create-only operation, so `updated` is always 0.
        return UpsertResult(created=created, updated=0, skipped=skipped)

    @staticmethod
    def _bulk_create_sync(parsed_rows: Sequence[PickBanRow], batch_size: int) -> int:
        """
        Performs the synchronous `bulk_create` operation. `ignore_conflicts=True`
        makes this idempotent, so re-running it on the same data does nothing.
        """
        if not parsed_rows:
            return 0
        try:
            with transaction.atomic():
                objs = [PickBan(**row.to_dict()) for row in parsed_rows]
                created_objs = PickBan.objects.bulk_create(objs, ignore_conflicts=True, batch_size=batch_size)
                created_count = len(created_objs)
                log.debug("Pick/Ban create complete.", created=created_count, total_in_batch=len(parsed_rows))
                return created_count
        except IntegrityError:
            log.error(
                "IntegrityError in PickBanDataHandler. This should be rare with ignore_conflicts=True.",
                exc_info=True,
            )
            raise
        except Exception:
            log.critical("Unexpected error in PickBanDataHandler.", exc_info=True)
            raise
