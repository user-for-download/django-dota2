from __future__ import annotations

import textwrap

from apps.core.conf import LATEST_PATCH_TS


def build_leagues_with_matches_ids_sql(
    *,
    limit: int = 50,
    patch: bool | None = False,
) -> str:
    """
    Builds a SQL query for leagues with their aggregated recent match IDs.

    The query finds leagues with recent matches within a specified time window,
    orders them by their most recent match, and aggregates the match IDs.

    Args:
        limit: Maximum number of leagues to return.
        patch: LATEST_PATCH_TS
    Returns:
        A formatted SQL query string.
    """

    time_filter = "True" if patch is None else f"m.start_time >= EXTRACT(EPOCH FROM TIMESTAMP '{LATEST_PATCH_TS}')"

    sql_template = """
                   WITH league_recent_matches AS (SELECT m.leagueid,
                                                         STRING_AGG(m.match_id::TEXT, ',' ORDER BY m.start_time DESC) AS match_ids,
                                                         MAX(m.start_time)                                            as max_start_time
                                                  FROM matches m
                                                  WHERE {time_filter}
                                                  GROUP BY m.leagueid)
                   SELECT l.name,
                          l.leagueid,
                          l.ticket,
                          l.banner,
                          l.tier,
                          lrm.match_ids
                   FROM leagues l
                            INNER JOIN league_recent_matches lrm ON l.leagueid = lrm.leagueid
                   ORDER BY l.leagueid ASC
                   LIMIT {limit} \
                   """

    query = textwrap.dedent(sql_template).format(
        time_filter=time_filter,
        limit=limit,
    )

    return query.strip()


def build_leagues_all() -> str:
    sql_template = """
                   SELECT l.*
                   FROM leagues as l
                   ORDER BY l.leagueid DESC
                   """
    query = textwrap.dedent(sql_template)
    return query.strip()
