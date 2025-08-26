# /home/ubuntu/dota/apps/teams/services/__init__.py
# ================================================================================
"""
Services for the 'teams' app.

This package provides high-level services for fetching, processing, and
persisting team data. By exposing key classes here, we allow for cleaner imports
in other modules.

Example:
    from apps.teams.services import TeamFetcher, TeamDataHandler
"""

from .team_data_handler import TeamDataHandler
from .team_fetcher import TeamFetcher

__all__ = [
    "TeamDataHandler",
    "TeamFetcher",
]
