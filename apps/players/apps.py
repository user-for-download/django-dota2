from django.apps import AppConfig


class PlayersConfig(AppConfig):
    """
    App configuration for the 'players' app.
    This controls metadata like name and admin display.
    """

    default_auto_field = "django.db.models.BigAutoField"  # Recommended for auto-increment fields in Django 3.2+
    name = "apps.players"  # Full dotted path to the app
    verbose_name = "Players"  # Human-readable name shown in the Django admin
