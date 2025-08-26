from __future__ import annotations

from typing import TYPE_CHECKING

from django.urls import path

from apps.core.views.hero_stats import (
    HeroBanStatsView,
    HeroGroupingStatsView,
    HeroPickStatsView,
    ScopedHeroRecommendView,
)

if TYPE_CHECKING:
    from django.urls.resolvers import URLPattern

urlpatterns: list[URLPattern] = [
    # URL for general hero stats (picks, bans, groupings)
    path("/picks", HeroPickStatsView.as_view(), name="hero-pick-stats"),
    path("/bans", HeroBanStatsView.as_view(), name="hero-ban-stats"),
    path("/groupings", HeroGroupingStatsView.as_view(), name="hero-grouping-stats"),
    # URL for draft recommendations
    path("/recommend", ScopedHeroRecommendView.as_view(), name="hero-recommend"),
]
