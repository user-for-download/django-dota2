# /home/ubuntu/dota/apps/teams/urls.py (Refactored)
# ================================================================================
"""URLConf for the Teams API (async views)."""

from __future__ import annotations

from django.urls import include, path

from .views import LeaderboardView, TeamDetailView, TeamListView

# The hero stats views are no longer imported from here.
# TeamHeroBanStatsView, TeamHeroGroupingStatsView, TeamHeroPickStatsView

app_name = "teams"

team_id_patterns = [
    path("", TeamDetailView.as_view(), name="detail"),
]

urlpatterns = [
    path("", TeamListView.as_view(), name="list"),
    path("/leaderboard", LeaderboardView.as_view(), name="leaderboard"),
    # The include path needs a trailing slash to correctly route sub-paths.
    path("/<int:team_id>", include(team_id_patterns)),
]
