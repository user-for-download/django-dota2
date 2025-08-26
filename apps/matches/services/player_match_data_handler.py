# /home/ubuntu/dota/apps/matches/services/player_match_data_handler.py
# ================================================================================
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

import structlog
from asgiref.sync import sync_to_async
from django.db import transaction
from django.db.utils import IntegrityError

from apps.core.datatype import UpsertResult, new_upsert_result
from apps.matches.models import PlayerMatch, PlayerMatchStats
from apps.matches.schemas.player_row import PlayerRow
from apps.players.models import Player

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

log: Final = structlog.get_logger(__name__).bind(handler="PlayerMatchDataHandler")


class PlayerMatchDataHandler:
    """
    Parses and persists player performance data for a single match, including
    both the main `PlayerMatch` record and its `PlayerMatchStats` side-table.
    """

    async def upsert_async(
        self,
        match_id: int,
        rows: Iterable[dict[str, Any]],
        *,
        bulk_size: int = 50,
    ) -> UpsertResult:
        """Asynchronously upserts a list of player performance rows for one match."""
        row_list = list(rows)
        if not row_list:
            return new_upsert_result()

        parsed_rows = [p for r in row_list if (p := PlayerRow.parse(r, match_id))]
        skipped = len(row_list) - len(parsed_rows)
        created = 0

        if parsed_rows:
            created = await sync_to_async(self._bulk_upsert_sync, thread_sensitive=True)(parsed_rows, bulk_size)

        return new_upsert_result(created=created, updated=len(parsed_rows) - created, skipped=skipped)

    @staticmethod
    def _ensure_players_exist_sync(rows: Sequence[PlayerRow]) -> None:
        """Ensures all referenced Player accounts exist before creating PlayerMatch records."""
        # 1. Collect all potential player IDs from the incoming data.
        all_player_ids = {int(p.player_id) for p in rows if p.player_id and int(p.player_id) > 0}
        if not all_player_ids:
            return

        # --- THE FIX: Perform an efficient "create-if-not-exists" ---
        # 2. Find which of these players *already exist* in the database with one query.
        existing_ids = set(
            Player.objects.filter(account_id__in=all_player_ids).values_list("account_id", flat=True),
        )

        # 3. Determine which players are new.
        new_ids = all_player_ids - existing_ids

        # 4. Bulk create *only* the new players.
        if new_ids:
            Player.objects.bulk_create(
                [Player(account_id=pid) for pid in new_ids],
                ignore_conflicts=True,  # Still good practice as a failsafe
            )
        # --- END FIX ---

    def _bulk_upsert_sync(self, parsed_rows: Sequence[PlayerRow], batch_size: int) -> int:
        """Performs the synchronous database upsert within an atomic transaction."""
        if not parsed_rows:
            return 0
        try:
            with transaction.atomic():
                self._ensure_players_exist_sync(parsed_rows)

                player_objs = [
                    PlayerMatch(match_id=row.match_id, player_slot=row.player_slot, **row.player_fields())
                    for row in parsed_rows
                ]
                created_objs = PlayerMatch.objects.bulk_create(
                    player_objs,
                    update_conflicts=True,
                    unique_fields=["match_id", "player_slot"],
                    update_fields=list(parsed_rows[0].player_fields().keys()),
                    batch_size=batch_size,
                )

                pk_map = {
                    r["player_slot"]: r["id"]
                    for r in PlayerMatch.objects.filter(
                        match_id=parsed_rows[0].match_id,
                        player_slot__in=[p.player_slot for p in parsed_rows],
                    ).values("id", "player_slot")
                }

                stats_objs = [
                    PlayerMatchStats(player_match_id=pk_map[row.player_slot], **row.stats_fields())
                    for row in parsed_rows
                    if row.player_slot in pk_map and row.stats_fields()
                ]
                if stats_objs:
                    PlayerMatchStats.objects.bulk_create(
                        stats_objs,
                        update_conflicts=True,
                        unique_fields=["player_match_id"],
                        update_fields=list(parsed_rows[0].stats_fields().keys()),
                        batch_size=batch_size,
                    )
                return len(created_objs)
        except IntegrityError as e:
            log.exception("IntegrityError in player_match_handler.", exc_info=e)
            raise
        except Exception:
            log.critical("Unexpected error in player_match_handler.", exc_info=True)
            raise
