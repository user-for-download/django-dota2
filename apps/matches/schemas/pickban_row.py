# /home/ubuntu/dota/apps/matches/schemas/pickban_row.py
# ================================================================================
"""Defines the PickBanRow dataclass, a schema for a single draft action."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class PickBanRow:
    """A DTO representing a single pick or ban in a match's draft phase."""

    match_id: int
    hero_id: int
    is_pick: bool
    team: int  # 0 for Radiant, 1 for Dire
    order: int

    @staticmethod
    def parse(src: dict, match_id: int) -> PickBanRow | None:
        """
        Factory method to safely parse a raw dictionary into a PickBanRow instance.
        """
        try:
            return PickBanRow(
                match_id=match_id,
                hero_id=int(src["hero_id"]),
                is_pick=bool(src["is_pick"]),
                team=int(src["team"]),
                order=int(src["order"]),
            )
        except (KeyError, TypeError, ValueError):
            return None

    def to_dict(self) -> dict:
        """Returns a dictionary representation of the dataclass."""
        return asdict(self)
