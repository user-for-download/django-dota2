"""
Main URL configuration optimized for Django 5.1 async views.
"""

# Django Imports
from django.conf import settings
from django.conf.urls.static import static
from django.urls import include, path
from django.views import defaults as default_views

# -----------------------------------------------------------------
# API URL Patterns
# Grouping API endpoints here makes versioning (e.g., v2) clean.
# -----------------------------------------------------------------
api_v1_patterns = [
    path("heroes", include("apps.core.urls")),
    path("matches", include("apps.matches.urls")),
    path("players", include("apps.players.urls")),
    path("teams", include("apps.teams.urls")),
    path("leagues", include("apps.leagues.urls")),
    # path("rankings/", include("apps.rankings.urls")),
    # path("analytics/", include("apps.analytics.urls")),
    # path("auth/", include("apps.api.urls")),
]

# -----------------------------------------------------------------
# Main URL Patterns
# -----------------------------------------------------------------
urlpatterns = [
    # --- API Versioning ---
    path("api/v1/", include(api_v1_patterns)),
]

# --- Global Error Handlers for API ---
# This ensures that any unhandled URL or server error will return a
# consistent JSON response instead of an HTML page.
handler404 = "apps.core.views.custom_handler.json_404_handler"
handler500 = "apps.core.views.custom_handler.json_500_handler"

# -----------------------------------------------------------------
# Development-Only Patterns (DEBUG=True)
# -----------------------------------------------------------------
if settings.DEBUG:
    # 1. Django Debug Toolbar
    if "debug_toolbar" in settings.INSTALLED_APPS:
        import debug_toolbar

        urlpatterns = [path("__debug__/", include(debug_toolbar.urls)), *urlpatterns]

    # 2. Static & Media Files
    # This is not strictly necessary for an API, but useful if you serve any
    # user-uploaded content during development.
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

    # 3. Human-friendly error page previews for development
    urlpatterns += [
        path("400/", default_views.bad_request, kwargs={"exception": Exception("Bad Request!")}),
        path("403/", default_views.permission_denied, kwargs={"exception": Exception("Permission Denied")}),
        path("404/", default_views.page_not_found, kwargs={"exception": Exception("Page not Found")}),
        path("500/", default_views.server_error),
    ]
