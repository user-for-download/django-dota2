# /home/ubuntu/dota/apps/matches/models/pick_ban.py
# ================================================================================
"""Model representing one pick or ban action during a match's draft phase."""

from __future__ import annotations

from django.core.validators import MaxValueValidator
from django.db import models

from apps.matches.conf import MAX_DRAFT_ORDER, Side


class PickBan(models.Model):
    """
    Represents a single hero pick or ban during the drafting phase of a match.
    """

    match = models.ForeignKey(
        "matches.Match",
        on_delete=models.CASCADE,
        related_name="draft",
    )
    hero = models.ForeignKey("core.Hero", on_delete=models.PROTECT)

    is_pick = models.BooleanField(help_text="True for a pick, False for a ban.")
    team = models.PositiveSmallIntegerField(
        choices=Side.choices,
        help_text="The team that made the pick/ban (0=Radiant, 1=Dire).",
    )
    order = models.PositiveSmallIntegerField(
        validators=[MaxValueValidator(MAX_DRAFT_ORDER)],
        help_text="The sequence of the pick/ban in the draft, starting from 0.",
    )

    class Meta:
        db_table = "picks_bans"
        ordering = ["match", "order"]
        # Ensures that each draft slot (order) within a match is unique.
        unique_together = [("match", "order")]
        indexes = [
            # Index to speed up analysis of hero pick/ban rates across all matches.
            models.Index(fields=["hero", "is_pick"]),
        ]

    def __str__(self) -> str:
        action = "Pick" if self.is_pick else "Ban"
        hero_name = getattr(self.hero, "localized_name", f"Hero {self.hero_id}")
        return f"{action}: {hero_name} by {self.get_team_display()} in Match {self.match_id}"
