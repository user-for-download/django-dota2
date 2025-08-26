# /home/ubuntu/dota/apps/rankings/conf.py
# ================================================================================
"""Configuration and constants for the rankings app."""

from __future__ import annotations

from typing import Final

# ─── Time Constants ────────────────────────────────────────────────────────────
WEEK_SECONDS: Final[int] = 7 * 24 * 60 * 60

# ─── Model & Query Constants ───────────────────────────────────────────────────
# These candidate columns are searched for in the PlayerMatch model to determine
# the timing bucket for scenario analysis.
TIMING_CANDIDATE_COLS: Final[tuple[str, ...]] = ("first_ts", "first_lane_ts", "lane_start_ts")

# This mapping is used to derive a coarse lane role from the player_slot if a
# dedicated 'lane_role' field is not available in the PlayerMatch model.
# Radiant cores: 0, 1, 2 | Dire cores: 128, 129, 130 -> Role 1 (Core)
# Radiant supports: 3, 4 | Dire supports: 131, 132 -> Role 4 (Support)
LANE_ROLE_MAPPING: Final[dict[int, tuple[int, ...]]] = {
    1: (0, 1, 2, 128, 129, 130),  # Cores
    4: (3, 4, 131, 132),  # Supports
}
