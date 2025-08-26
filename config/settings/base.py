# ruff: noqa: ERA001
"""
Base settings for the Dota project.

These settings are suitable for production.
Local development settings should override these in 'local.py'.
"""

from pathlib import Path

import environ
from pydantic_settings import BaseSettings
from config.log import LOGGING

# Project structure
BASE_DIR = Path(__file__).resolve(strict=True).parent.parent.parent
APPS_DIR = BASE_DIR

# Environment variables setup
env = environ.Env()
READ_DOT_ENV_FILE = env.bool("DJANGO_READ_DOT_ENV_FILE", default=False)
if READ_DOT_ENV_FILE:
    env.read_env(str(BASE_DIR / ".env"))

# GENERAL
# ------------------------------------------------------------------------------
DEBUG = env.bool("DJANGO_DEBUG", False)
TIME_ZONE = "Europe/Moscow"
LANGUAGE_CODE = "en-us"
SITE_ID = 1
USE_I18N = True
USE_TZ = True
LOCALE_PATHS = [str(BASE_DIR / "locale")]

# URLS & APPLICATIONS
# ------------------------------------------------------------------------------
ROOT_URLCONF = "config.urls"
WSGI_APPLICATION = "config.wsgi.application"
ASGI_APPLICATION = "config.asgi.application"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# DATABASES
# ------------------------------------------------------------------------------
DATABASES = {
    "default": env.db_url(
        "DATABASE_URL",
        default="postgres://myuser:mypassword@localhost:5432/mydb",
    ),
}
DATABASES["default"]["ATOMIC_REQUESTS"] = False
DATABASES["default"]["CONN_MAX_AGE"] = env.int("CONN_MAX_AGE", default=60)

# APPS
# ------------------------------------------------------------------------------
DJANGO_APPS = [
    # "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    # "django.contrib.sessions",
    # "django.contrib.sites",
    # "django.contrib.messages",
    "django.contrib.staticfiles",
    # "django.forms",
]
THIRD_PARTY_APPS = [
    # Add third-party apps here
]
LOCAL_APPS = [
    "apps.core.apps.CoreConfig",
    "apps.teams.apps.TeamsConfig",
    "apps.players.apps.PlayersConfig",
    "apps.leagues.apps.LeaguesConfig",
    "apps.matches.apps.MatchesConfig",
    "apps.rankings.apps.RankingsConfig",
    # "dota.users",
]
INSTALLED_APPS = DJANGO_APPS + THIRD_PARTY_APPS + LOCAL_APPS

# AUTHENTICATION & PASSWORDS
# ------------------------------------------------------------------------------
# AUTH_USER_MODEL = "users.User"
AUTHENTICATION_BACKENDS = ["django.contrib.auth.backends.ModelBackend"]
PASSWORD_HASHERS = [
    "django.contrib.auth.hashers.Argon2PasswordHasher",
    "django.contrib.auth.hashers.PBKDF2PasswordHasher",
]
AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
]

# MIDDLEWARE
# ------------------------------------------------------------------------------
MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.locale.LocaleMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

# STATIC & MEDIA
# ------------------------------------------------------------------------------
STATIC_ROOT = str(BASE_DIR / "staticfiles")
STATIC_URL = "/static/"
STATICFILES_DIRS = [str(APPS_DIR / "static")]
STATICFILES_FINDERS = [
    "django.contrib.staticfiles.finders.FileSystemFinder",
    "django.contrib.staticfiles.finders.AppDirectoriesFinder",
]
STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"
MEDIA_ROOT = str(APPS_DIR / "media")
MEDIA_URL = "/media/"

# TEMPLATES & FORMS
# ------------------------------------------------------------------------------
TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [str(APPS_DIR / "templates")],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]
FORM_RENDERER = "django.forms.renderers.TemplatesSetting"

# SECURITY
# ------------------------------------------------------------------------------
SESSION_COOKIE_HTTPONLY = True
CSRF_COOKIE_HTTPONLY = True
X_FRAME_OPTIONS = "DENY"

# ADMIN
# ------------------------------------------------------------------------------
ADMIN_URL = env("DJANGO_ADMIN_URL", default="admin/")
ADMINS = [("Daniel Roy Greenfeld", "daniel-roy-greenfeld@example.com")]
MANAGERS = ADMINS


# Cache DB (index 1)
REDIS_CACHE_URL = env(
    "REDIS_CACHE_URL",
    default="redis://redis:6379/1",
)

FASTSTREAM_REDIS_URL = env(
    "FASTSTREAM_REDIS_URL",
    default="redis://redis:6379/2",
)

CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": env("REDIS_CACHE_URL", default="redis://redis:6379/1"),
        "OPTIONS": {
            "CLIENT_CLASS": "django_redis.client.DefaultClient",
            "IGNORE_EXCEPTIONS": True,
        },
    },
}

# DOTA API CONFIG
# ------------------------------------------------------------------------------


class DotaApiSettings(BaseSettings):
    EXPLORER_URL: str = "https://api.opendota.com/api/explorer"
    TIMEOUT_S: int = 45

    RETRY_CONFIG: dict = {
        "max_retries": 3,
        "base_delay_s": 2.0,  # Changed from base_delay
        "max_delay_s": 60.0,  # Changed from max_delay
        "jitter_factor": 0.5,  # Added for completeness
    }

    model_config = {"frozen": True}


DOTA_API_CONFIG = DotaApiSettings()  # ⇐ attribute-style access

SECRET_KEY = env("DJANGO_SECRET_KEY", default=None)

BATCH_SIZE = 200
MAX_PUBLISH_CONCURRENCY = 10
APPEND_SLASH = False
