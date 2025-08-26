# /home/ubuntu/dota/apps/teams/services/queries.py
# ================================================================================
"""
SQL query builders for team data fetching.
"""

from __future__ import annotations

import textwrap

from apps.core.conf import LATEST_PATCH_TS


def build_teams_with_matches_ids_sql(
    *,
    limit: int = 50,
    min_rating: int = 1000,
    days: int | None = 30,
) -> str:
    """
    Builds a SQL query to fetch teams, their ratings, and aggregated recent match IDs.

    The query joins teams with their ratings and recent matches, filtering by a
    minimum rating and a specified time window.

    Args:
        limit: Maximum number of teams to return.
        min_rating: The minimum team_rating required for a team to be included.
        days: The number of days to look back for recent matches. If set to None,
              it filters matches since the latest patch timestamp.

    Returns:
        A formatted SQL query string.
    """
    if days is not None and days > 0:
        time_filter = f"m.start_time >= EXTRACT(EPOCH FROM (NOW() - INTERVAL '{days}days'))"
    elif days is None:
        time_filter = f"m.start_time >= EXTRACT(EPOCH FROM TIMESTAMP '{LATEST_PATCH_TS}')"
    else:
        time_filter = "True"

    sql_template = """
                   WITH team_recent_matches AS (SELECT tm.team_id,
                                                       m.match_id,
                                                       m.start_time
                                                FROM matches AS m
                                                         JOIN
                                                     team_match AS tm ON m.match_id = tm.match_id
                                                WHERE {time_filter}),

                        team_match_aggregates AS (SELECT trm.team_id,
                                                         STRING_AGG(trm.match_id::TEXT, ',' ORDER BY trm.start_time DESC) AS match_ids
                                                  FROM team_recent_matches AS trm
                                                  GROUP BY trm.team_id)

                   SELECT t.team_id,
                          t.name,
                          t.tag,
                          t.logo_url,
                          tr.rating,
                          tr.wins,
                          tr.losses,
                          tr.last_match_time,
                          tma.match_ids
                   FROM teams AS t
                            JOIN
                        team_rating AS tr ON t.team_id = tr.team_id
                            INNER JOIN
                        team_match_aggregates AS tma ON t.team_id = tma.team_id
                   WHERE tr.rating >= {min_rating}
                   ORDER BY
                       tr.rating DESC
                   LIMIT {limit};
                   """

    query = textwrap.dedent(sql_template).format(
        time_filter=time_filter,
        min_rating=min_rating,
        limit=limit,
    )

    return query.strip()


def build_teams_all() -> str:
    sql_template = """
                   SELECT t.team_id,
                          t.name,
                          t.tag,
                          t.logo_url,
                          tr.rating,
                          tr.wins,
                          tr.losses,
                          tr.last_match_time
                   FROM teams AS t
                            JOIN
                        team_rating AS tr ON t.team_id = tr.team_id

                   ORDER BY tr.rating DESC
                   """
    query = textwrap.dedent(sql_template)
    return query.strip()
