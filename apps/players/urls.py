# /home/ubuntu/dota/apps/players/urls.py (Refactored)
# ================================================================================
from __future__ import annotations

from django.urls import include, path

from .views import PlayerDetailView, PlayerListView, PlayerRankTierView, PlayerRatingHistoryView, ProPlayerListView

# The redundant hero stats views are no longer imported.

app_name = "players"

player_id_patterns = [
    path("", PlayerDetailView.as_view(), name="player-detail"),
    path("/ratings", PlayerRatingHistoryView.as_view(), name="player-ratings"),
    path("/rank", PlayerRankTierView.as_view(), name="player-rank"),
]

urlpatterns = [
    path("", PlayerListView.as_view(), name="player-list"),
    path("/pro", ProPlayerListView.as_view(), name="pro-player-list"),
    path("/<int:account_id>", include(player_id_patterns)),
]
