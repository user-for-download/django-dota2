from .base import *
from .base import INSTALLED_APPS, MIDDLEWARE, env

DEBUG = False
SECRET_KEY = env(
    "DJANGO_SECRET_KEY",
    default="pT85aTBAHkX8Rffu9aHAdX2sOUey8dJqDUIcT43D95fRkxc1uFqkjNHe0fDDEt4Z",
)
ALLOWED_HOSTS = ["localhost", "0.0.0.0", "127.0.0.1", "rk.binetc.store"]

EMAIL_BACKEND = env(
    "DJANGO_EMAIL_BACKEND",
    default="django.core.mail.backends.console.EmailBackend",
)

if DEBUG:
    INSTALLED_APPS += ['debug_toolbar', 'django_extensions']
    MIDDLEWARE += ["debug_toolbar.middleware.DebugToolbarMiddleware"]
    DEBUG_TOOLBAR_CONFIG = {
        "DISABLE_PANELS": ["debug_toolbar.panels.redirects.RedirectsPanel"],
        "SHOW_TEMPLATE_CONTEXT": True,
    }
