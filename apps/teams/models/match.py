# /home/ubuntu/dota/apps/teams/models/match.py
# ================================================================================
"""
TeamMatch: A through model linking Teams to Matches, including side information.
Optimized for bulk operations and high-performance asynchronous queries.
"""

from __future__ import annotations

from django.db import models
from django.db.models import Prefetch
from django.utils.translation import gettext_lazy as _


class TeamMatchQuerySet(models.QuerySet["TeamMatch"]):
    """Optimized QuerySet for the TeamMatch model."""

    def with_team_info(self) -> TeamMatchQuerySet:
        """Eager-loads related team data to prevent N+1 queries."""
        return self.select_related("team")

    def recent_matches(self, limit: int = 50) -> TeamMatchQuerySet:
        """Gets the most recent N matches for a team."""
        return self.order_by("-match_id")[:limit]

    def by_side(self, *, is_radiant: bool) -> TeamMatchQuerySet:
        """Filters matches by the side the team played on."""
        return self.filter(radiant=is_radiant)

    def with_results(self) -> TeamMatchQuerySet:
        """
        Prefetches related match results to avoid N+1 queries when calculating
        win/loss status. This is a critical performance optimization.

        Without this, calling `get_result_efficient` on each TeamMatch instance
        in a loop would trigger a separate database query for each match.
        """
        from apps.matches.models import Match

        return self.select_related("team").prefetch_related(
            Prefetch(
                "match",
                queryset=Match.objects.only("match_id", "winner"),
                # Cache the prefetched object on a private attribute for internal use.
                to_attr="_prefetched_match_result",
            ),
        )


class TeamMatch(models.Model):
    """
    A "through" model connecting a `Team` to a `Match`.
    It stores which team played in which match and on which side (Radiant/Dire).
    """

    team = models.ForeignKey(
        "Team",
        on_delete=models.PROTECT,  # Prevent deleting a team if it has match history.
        db_column="team_id",
        related_name="team_matches",
    )
    match = models.ForeignKey(
        "matches.Match",
        on_delete=models.CASCADE,  # If a match is deleted, this link becomes irrelevant.
        db_column="match_id",
        related_name="team_match_entries",
    )
    radiant = models.BooleanField(help_text=_("True if the team was on the Radiant side, False for Dire."))
    created_at = models.DateTimeField(auto_now_add=True)

    objects = TeamMatchQuerySet.as_manager()

    class Meta:
        db_table = "team_match"
        verbose_name = _("team match")
        verbose_name_plural = _("team matches")
        ordering = ["-match_id"]
        constraints = [
            models.UniqueConstraint(fields=["team", "match"], name="uniq_team_match"),
        ]
        indexes = [
            # Index to speed up fetching recent matches for a specific team.
            models.Index(fields=["team", "-match_id"], name="team_match_team_recent_idx"),
        ]

    def __str__(self) -> str:
        side = "Radiant" if self.radiant else "Dire"
        return f"Team {self.team_id} in Match {self.match_id} ({side})"

    def get_result_efficient(self) -> str | None:
        prefetched_match_list = getattr(self, "_prefetched_match_result", None)
        if prefetched_match_list and isinstance(prefetched_match_list, list) and len(prefetched_match_list) > 0:
            match_data = prefetched_match_list[0]
            if match_data.winner is not None:
                is_winner = (self.radiant and match_data.winner == 1) or (not self.radiant and match_data.winner == 0)
                return "Win" if is_winner else "Loss"
        return None

    def to_dict(self, *, include_result: bool = False) -> dict:
        """Serializes the model instance to a dictionary for API responses."""
        data = {
            "team_id": self.team_id,
            "match_id": self.match_id,
            "side": "Radiant" if self.radiant else "Dire",
            "created_at": self.created_at.isoformat(),
        }
        if include_result:
            data["result"] = self.get_result_efficient()
        return data
