# ================================================================================
from django.apps import AppConfig


class TeamsConfig(AppConfig):
    """
    App configuration for the 'teams' app.
    Specifies the default auto field and provides a verbose name for the Django admin.
    """

    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.teams"
    verbose_name = "Teams"
