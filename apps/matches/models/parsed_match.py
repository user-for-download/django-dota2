# /home/ubuntu/dota/apps/matches/models/parsed_match.py
# ================================================================================
"""Tracks the processing and parsing status of matches."""

from __future__ import annotations

from django.db import models


class ParseStatus(models.TextChoices):
    """Enumeration for the processing status of a match replay."""

    PENDING = "pending", "Pending"
    PARSING = "parsing", "Parsing"
    COMPLETED = "completed", "Completed"
    FAILED = "failed", "Failed"
    ARCHIVED = "archived", "Archived"


class ParsedMatch(models.Model):
    """
    Tracks the ingestion and parsing status for each match, forming a crucial
    part of the data processing pipeline.
    """

    match = models.OneToOneField(
        "matches.Match",
        on_delete=models.CASCADE,
        primary_key=True,
        related_name="parse_info",
    )
    parse_status = models.CharField(
        max_length=20,
        choices=ParseStatus.choices,
        default=ParseStatus.PENDING,
        db_index=True,
    )
    version = models.PositiveIntegerField(null=True, blank=True, help_text="The version of the parser used.")
    error_message = models.TextField(blank=True, default="", help_text="Stores any error message if parsing failed.")

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "parsed_matches"
        ordering = ["-match__start_time"]
        verbose_name = "Parsed Match Status"
        verbose_name_plural = "Parsed Match Statuses"

    def __str__(self) -> str:
        return f"Match {self.match_id} [{self.get_parse_status_display()}]"
