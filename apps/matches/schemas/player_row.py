# /home/ubuntu/dota/apps/matches/schemas/player_row.py
# ================================================================================
"""
Defines the PlayerRow dataclass, a schema for structuring raw player performance
data within a single match.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from typing import Any


@dataclass(frozen=True, slots=True)
class PlayerRow:
    """
    A DTO for a player's performance in one match. It separates fields for the
    main `PlayerMatch` table from the less-frequently accessed `PlayerMatchStats` table.
    """

    # Identifiers
    match_id: int
    player_slot: int
    player_id: int | None = None  # Can be None for anonymous players.
    hero_id: int | None = None

    # Core `PlayerMatch` fields
    kills: int | None = None
    deaths: int | None = None
    assists: int | None = None
    last_hits: int | None = None
    denies: int | None = None
    gold_per_min: int | None = None
    xp_per_min: int | None = None
    level: int | None = None
    net_worth: int | None = None
    items: list[int | None] | None = None
    backpack: list[int | None] | None = None
    item_neutral: int | None = None

    # `PlayerMatchStats` fields (side-table)
    hero_damage: int | None = None
    tower_damage: int | None = None
    hero_healing: int | None = None
    stuns: float | None = None
    obs_placed: int | None = None
    sen_placed: int | None = None
    creeps_stacked: int | None = None
    rune_pickups: int | None = None
    ability_uses: dict[str, Any] | None = None
    damage_targets: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Returns a dictionary representation of the entire dataclass."""
        return asdict(self)

    def player_fields(self) -> dict[str, Any]:
        """Returns a dictionary of fields belonging to the `PlayerMatch` model."""
        included = {
            "player_id",
            "hero_id",
            "kills",
            "deaths",
            "assists",
            "last_hits",
            "denies",
            "gold_per_min",
            "xp_per_min",
            "level",
            "net_worth",
            "items",
            "backpack",
            "item_neutral",
        }
        return {
            f.name: getattr(self, f.name)
            for f in fields(self)
            if f.name in included and getattr(self, f.name) is not None
        }

    def stats_fields(self) -> dict[str, Any]:
        """Returns a dictionary of fields belonging to the `PlayerMatchStats` model."""
        included = {
            "hero_damage",
            "tower_damage",
            "hero_healing",
            "stuns",
            "obs_placed",
            "sen_placed",
            "creeps_stacked",
            "rune_pickups",
            "ability_uses",
            "damage_targets",
        }
        return {
            f.name: getattr(self, f.name)
            for f in fields(self)
            if f.name in included and getattr(self, f.name) is not None
        }

    @staticmethod
    def parse(src: dict[str, Any], match_id: int) -> PlayerRow | None:
        """
        Factory method to safely parse a raw dictionary into a PlayerRow instance.
        Requires the parent `match_id` to be passed in.
        """
        try:
            slot = int(src["player_slot"])
        except (KeyError, TypeError, ValueError):
            return None

        # The `items` field in the source API is named `item_0` to `item_5`.
        # This part of the original code was missing but is crucial.
        items = [src.get(f"item_{i}") for i in range(6)]
        backpack_items = [src.get(f"backpack_{i}") for i in range(3)]

        return PlayerRow(
            match_id=match_id,
            player_slot=slot,
            player_id=src.get("account_id"),
            hero_id=src.get("hero_id"),
            kills=src.get("kills"),
            deaths=src.get("deaths"),
            assists=src.get("assists"),
            last_hits=src.get("last_hits"),
            denies=src.get("denies"),
            gold_per_min=src.get("gold_per_min"),
            xp_per_min=src.get("xp_per_min"),
            level=src.get("level"),
            net_worth=src.get("net_worth"),
            items=items,
            backpack=backpack_items,
            item_neutral=src.get("item_neutral"),
            hero_damage=src.get("hero_damage"),
            tower_damage=src.get("tower_damage"),
            hero_healing=src.get("hero_healing"),
            stuns=src.get("stuns"),
            obs_placed=src.get("obs_placed"),
            sen_placed=src.get("sen_placed"),
            creeps_stacked=src.get("creeps_stacked"),
            rune_pickups=src.get("rune_pickups"),
            ability_uses=src.get("ability_uses"),
            damage_targets=src.get("damage_targets"),
        )
