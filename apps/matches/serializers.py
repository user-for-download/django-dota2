from __future__ import annotations

from typing import TYPE_CHECKING, Any

from django.db.models import Prefetch

from .models import Match, MatchPatch, MatchStats, PlayerMatch

if TYPE_CHECKING:
    from collections.abc import Sequence


class PlayerMatchSerializer:
    """Flat scoreboard row."""

    @staticmethod
    def serialize(pm: PlayerMatch, *, include_hero: bool = True) -> dict[str, Any]:
        data: dict[str, Any] = {
            "player_slot": pm.player_slot,
            "kills": pm.kills,
            "deaths": pm.deaths,
            "assists": pm.assists,
            "kda": pm.kda_ratio,
            "last_hits": pm.last_hits,
            "denies": pm.denies,
            "gpm": pm.gold_per_min,
            "xpm": pm.xp_per_min,
            "level": pm.level,
            "is_radiant": pm.is_radiant,
        }
        if include_hero:
            data["hero"] = {
                "id": pm.hero_id,
                "name": pm.hero.localized_name,
            }
        if pm.player_id:
            data["player_id"] = pm.player_id
            data["player_name"] = getattr(pm.player, "personaname", None)
        data["items"] = {
            "main": pm.items,
            "backpack": pm.backpack,
            "neutral": pm.item_neutral,
        }
        return data

    @classmethod
    def serialize_many(
        cls,
        rows: Sequence[PlayerMatch],
        *,
        include_hero: bool = True,
    ) -> list[dict[str, Any]]:
        return [cls.serialize(r, include_hero=include_hero) for r in rows]


class MatchSerializer:
    """Lightweight match representation."""

    # ─────────────────────────── Single ────────────────────────────
    @staticmethod
    def serialize_match(
        m: Match,
        *,
        include_stats: bool = False,
        include_patch: bool = False,
        include_players: bool = False,
    ) -> dict[str, Any]:
        data: dict[str, Any] = {
            "match_id": m.match_id,
            "start_time": m.start_time,
            "duration": m.duration,
            "winner": m.winner,
            "radiant_score": m.radiant_score,
            "dire_score": m.dire_score,
        }

        if include_stats and hasattr(m, "stats") and isinstance(m.stats, MatchStats):
            s = m.stats
            data["stats"] = {
                "tower_status": {
                    "radiant": s.tower_status_radiant,
                    "dire": s.tower_status_dire,
                },
                "barracks_status": {
                    "radiant": s.barracks_status_radiant,
                    "dire": s.barracks_status_dire,
                },
                "first_blood_time": s.first_blood_time,
                "game_mode": s.game_mode,
                "lobby_type": s.lobby_type,
            }

        if include_patch and hasattr(m, "patch_info") and isinstance(m.patch_info, MatchPatch):
            p = m.patch_info
            data["patch"] = {
                "name": p.patch,
                "major": p.patch_major,
                "minor": p.patch_minor,
            }

        if include_players and hasattr(m, "player_matches"):
            data["players"] = PlayerMatchSerializer.serialize_many(
                list(m.player_matches.all()),
            )

        return data

    # ─────────────────────────── Multiple ──────────────────────────
    @classmethod
    def serialize_matches(
        cls,
        matches: Sequence[Match],
        *,
        include_stats: bool = False,
        include_patch: bool = False,
    ) -> list[dict[str, Any]]:
        return [
            cls.serialize_match(
                m,
                include_stats=include_stats,
                include_patch=include_patch,
            )
            for m in matches
        ]

    # ─────────────────────── Prefetch helper ───────────────────────
    @staticmethod
    def base_queryset(
        *,
        stats: bool = False,
        patch: bool = False,
        players: bool = False,
    ):
        qs = Match.objects.all()
        if stats:
            qs = qs.select_related("stats")
        if patch:
            qs = qs.select_related("patch_info")
        if players:
            qs = qs.prefetch_related(
                Prefetch("player_matches", queryset=PlayerMatch.objects.select_related("hero", "player")),
            )
        return qs
