# /home/ubuntu/dota/apps/leagues/models/__init__.py
# ================================================================================
"""
Public API for the leagues models. This re-exports the main League model
for convenient importing from other apps.
"""
from .league import League, LeagueQuerySet

__all__ = ["League", "LeagueQuerySet"]
