# /home/ubuntu/dota/apps/matches/models/match_patch.py
# ================================================================================
"""Associates a Match with its specific game version (patch)."""

from __future__ import annotations

from django.db import models
from django.db.models import GeneratedField, IntegerField, Value
from django.db.models.functions import Cast, StrIndex, Substr


class MatchPatch(models.Model):
    """
    A one-to-one extension of the Match model that stores the game patch
    version and provides generated fields for efficient version filtering.
    """

    match = models.OneToOneField(
        "matches.Match",
        on_delete=models.CASCADE,
        primary_key=True,
        related_name="patch_info",
    )
    patch = models.CharField(max_length=20, db_index=True, help_text="The full patch string, e.g., '7.35c'.")

    # --- Generated Fields (calculated by the database) ---
    patch_major = GeneratedField(
        expression=Cast(
            Substr("patch", 1, StrIndex("patch", Value(".")) - 1),
            output_field=IntegerField(),
        ),
        output_field=models.PositiveIntegerField(),
        db_persist=True,
        db_comment="Major version (e.g., 7 from '7.35c').",
    )
    patch_minor = GeneratedField(
        expression=Cast(
            Substr(
                "patch",
                StrIndex("patch", Value(".")) + 1,
                # Extract numbers before any letter (e.g., '35' from '35c')
                # This requires a more complex regex in a real scenario, but for now
                # we assume a simple numeric minor version for this expression.
                2,
            ),
            output_field=IntegerField(),
        ),
        output_field=models.PositiveIntegerField(),
        db_persist=True,
        db_comment="Minor version (e.g., 35 from '7.35c').",
    )
    # NOTE: The Substr expression for `patch_minor` is simplified. A robust
    # solution for versions like '7.35c' would need `regexp_replace` or similar
    # to strip non-numeric characters, which is more complex but possible.

    class Meta:
        db_table = "match_patch"
        verbose_name = "Match Patch"
        verbose_name_plural = "Match Patches"
        indexes = [
            # Index to accelerate filtering by major and minor patch versions.
            models.Index(fields=["patch_major", "patch_minor"]),
        ]

    def __str__(self) -> str:
        return f"Match {self.match_id} – Patch {self.patch}"
