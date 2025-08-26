# /home/ubuntu/dota/apps/leagues/urls.py (Refactored)
# ================================================================================
"""
URLConf for the Leagues API.
Pattern: /api/v1/leagues/...
"""

from __future__ import annotations

from django.urls import include, path

from .views import LeagueDetailView, LeagueListView

app_name = "leagues"

league_id_patterns = [
    # The root of this subgroup is an empty string, correctly matching /leagues/<id>/
    path("", LeagueDetailView.as_view(), name="detail"),
]

urlpatterns = [
    path("", LeagueListView.as_view(), name="list"),
    path("/<int:league_id>", include(league_id_patterns)),
]
