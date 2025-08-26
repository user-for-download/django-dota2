# File: /home/ubuntu/dota/apps/matches/apps.py
# ================================================================================
from django.apps import AppConfig


class MatchesConfig(AppConfig):
    """Configuration for the matches app."""

    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.matches"
    verbose_name = "Matches"
