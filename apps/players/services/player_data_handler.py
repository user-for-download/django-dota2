# /home/ubuntu/dota/apps/players/services/player_data_handler.py
# ================================================================================
"""
High-volume upsert pipeline for players, notable player profiles, and their
associated teams.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import islice
from typing import TYPE_CHECKING, Any, ClassVar

import structlog
from asgiref.sync import sync_to_async
from django.db import IntegrityError, transaction

from apps.core.datatype import UpsertResult, new_upsert_result
from apps.players.models import NotablePlayer, Player
from apps.teams.models import Team
from common.time_utils import to_datetime_aware_safe

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence
    from typing import Self

log = structlog.get_logger(__name__).bind(handler="PlayerDataHandler")


@dataclass(slots=True, frozen=True)
class _ValidatedPlayerRow:
    """A validated, internal DTO for a comprehensive player data row."""

    account_id: int
    steamid: str | None = None
    avatar: str | None = None
    avatarmedium: str | None = None
    avatarfull: str | None = None
    profileurl: str | None = None
    personaname: str | None = None
    last_login: Any | None = None
    full_history_time: Any | None = None
    cheese: int | None = None
    fh_unavailable: bool | None = None
    loccountrycode: str | None = None
    last_match_time: Any | None = None
    plus: bool | None = None
    name: str | None = None
    country_code: str | None = None
    fantasy_role: int | None = None
    team_id: int | None = None
    is_locked: bool | None = None
    is_pro: bool | None = None
    locked_until: Any | None = None
    is_current_team_member: bool | None = None
    team_name: str | None = None
    team_tag: str | None = None

    PLAYER_FIELDS: ClassVar[tuple[str, ...]] = (
        "steamid",
        "avatar",
        "avatarmedium",
        "avatarfull",
        "profileurl",
        "personaname",
        "last_login",
        "full_history_time",
        "cheese",
        "fh_unavailable",
        "loccountrycode",
        "last_match_time",
        "plus",
    )
    NOTABLE_FIELDS: ClassVar[tuple[str, ...]] = (
        "name",
        "country_code",
        "fantasy_role",
        "team_id",
        "is_locked",
        "is_pro",
        "locked_until",
        "is_current_team_member",
    )
    TEAM_FIELDS: ClassVar[tuple[str, ...]] = ("name", "tag")

    @classmethod
    def parse(cls, src: dict[str, Any]) -> Self | None:
        try:
            account_id = int(src["account_id"])
        except (KeyError, TypeError, ValueError):
            log.warning("Skipping row: missing or invalid 'account_id'", raw_data=src)
            return None
        team_id = src.get("team_id")
        return cls(
            account_id=account_id,
            steamid=src.get("steamid"),
            avatar=src.get("avatar"),
            avatarmedium=src.get("avatarmedium"),
            avatarfull=src.get("avatarfull"),
            profileurl=src.get("profileurl"),
            personaname=src.get("personaname"),
            last_login=to_datetime_aware_safe(src.get("last_login")),
            full_history_time=to_datetime_aware_safe(src.get("full_history_time")),
            cheese=src.get("cheese"),
            fh_unavailable=src.get("fh_unavailable"),
            loccountrycode=src.get("loccountrycode"),
            last_match_time=to_datetime_aware_safe(src.get("last_match_time")),
            plus=src.get("plus"),
            name=src.get("pro_name") or src.get("name"),
            country_code=src.get("country_code"),
            fantasy_role=src.get("fantasy_role"),
            team_id=team_id if team_id != 0 else None,
            is_locked=src.get("is_locked"),
            is_pro=src.get("is_pro"),
            locked_until=to_datetime_aware_safe(src.get("locked_until")),
            is_current_team_member=bool(team_id and team_id != 0),
            team_name=src.get("team_name"),
            team_tag=src.get("team_tag"),
        )

    def player_kwargs(self) -> dict[str, Any]:
        return {k: v for k in self.PLAYER_FIELDS if (v := getattr(self, k)) is not None}

    def notable_kwargs(self) -> dict[str, Any]:
        return {k: v for k in self.NOTABLE_FIELDS if (v := getattr(self, k)) is not None}

    def team_kwargs(self) -> dict[str, Any] | None:
        if not self.team_id or not self.team_name:
            return None
        return {"name": self.team_name, "tag": self.team_tag}


class PlayerDataHandler:
    """Converts API rows and performs bulk upserts for Player and related models."""

    async def upsert_async(
        self,
        rows: Iterable[dict[str, Any]],
        *,
        bulk_size: int = 1_000,
        chunk_size: int = 5_000,
    ) -> UpsertResult:
        rows_iter = iter(rows)
        total_result = new_upsert_result()

        while chunk := list(islice(rows_iter, chunk_size)):
            parsed_rows = [p for r in chunk if (p := _ValidatedPlayerRow.parse(r))]
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
    def _bulk_upsert_sync(parsed_rows: Sequence[_ValidatedPlayerRow], bulk_size: int) -> int:
        account_ids = [row.account_id for row in parsed_rows]
        existing_count = Player.objects.filter(account_id__in=account_ids).count()

        players = [Player(account_id=row.account_id, **row.player_kwargs()) for row in parsed_rows]
        notables = [NotablePlayer(player_id=row.account_id, **row.notable_kwargs()) for row in parsed_rows]

        teams: dict[int, Team] = {}
        for row in parsed_rows:
            if row.team_id and row.team_id not in teams and (t_kwargs := row.team_kwargs()):
                teams[row.team_id] = Team(team_id=row.team_id, **t_kwargs)

        try:
            with transaction.atomic():
                if teams:
                    Team.objects.bulk_create(
                        list(teams.values()),
                        update_conflicts=True,
                        unique_fields=["team_id"],
                        update_fields=_ValidatedPlayerRow.TEAM_FIELDS,
                        batch_size=bulk_size,
                    )
                Player.objects.bulk_create(
                    players,
                    update_conflicts=True,
                    unique_fields=["account_id"],
                    update_fields=_ValidatedPlayerRow.PLAYER_FIELDS,
                    batch_size=bulk_size,
                )
                NotablePlayer.objects.bulk_create(
                    notables,
                    update_conflicts=True,
                    unique_fields=["player_id"],
                    update_fields=_ValidatedPlayerRow.NOTABLE_FIELDS,
                    batch_size=bulk_size,
                )

            # ✅ Correct: new = total - previously existing
            return len(parsed_rows) - existing_count
        except IntegrityError as e:
            log.exception("player_bulk_upsert_integrity_error", exc_info=e)
            return 0
        except Exception as e:
            log.critical("player_bulk_upsert_unexpected_error", exc_info=e)
            raise
