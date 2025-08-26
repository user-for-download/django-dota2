"""
Django-app registry for `apps.leagues`.
"""

from django.apps import AppConfig


class LeaguesConfig(AppConfig):
    """App registration."""

    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.leagues"
    verbose_name = "Leagues"
