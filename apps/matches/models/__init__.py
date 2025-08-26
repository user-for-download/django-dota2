# /home/ubuntu/dota/apps/matches/models/__init__.py
# ================================================================================
"""
Dota 2 match-data models (Django 5.2).

This package defines the database schema for all match-related data. The schema
is normalized to separate high-traffic tables (like `Match`) from larger,
less-frequently accessed data (like `MatchStats`). This `__init__.py` file
re-exports the primary models for easy access from other apps.
"""

from __future__ import annotations

from .match import Match, MatchManager, MatchQuerySet
from .match_patch import MatchPatch
from .match_stats import MatchStats
from .parsed_match import ParsedMatch
from .pick_ban import PickBan
from .player_match import PlayerMatch
from .player_match_stats import PlayerMatchStats
from .public_match import PublicMatch

__all__ = [
    "Match",
    "MatchManager",
    "MatchPatch",  # REFACTOR: Added missing model.
    "MatchQuerySet",
    "MatchStats",
    "ParsedMatch",
    "PickBan",
    "PlayerMatch",
    "PlayerMatchStats",
    "PublicMatch",
]
