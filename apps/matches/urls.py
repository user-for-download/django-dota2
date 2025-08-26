# apps/matches/urls.py (Final)
from __future__ import annotations

from django.urls import include, path

from .views import (
    FilteredMatchListView,  # The new, powerful list view
    LongestMatchesView,
    MatchComprehensiveView,
    MatchDetailView,
    MatchPlayersView,
)

app_name = "matches"

match_id_patterns = [
    path("", MatchDetailView.as_view(), name="match-detail"),
    path("/players", MatchPlayersView.as_view(), name="match-players"),
    path("/full", MatchComprehensiveView.as_view(), name="match-comprehensive"),
]

urlpatterns = [
    # The new consolidated endpoint for listing matches
    path("", FilteredMatchListView.as_view(), name="match-list"),
    path("/longest", LongestMatchesView.as_view(), name="longest-matches"),
    path("/<int:match_id>", include(match_id_patterns)),
]
