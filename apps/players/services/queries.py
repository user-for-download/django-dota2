import textwrap

from apps.core.conf import LATEST_PATCH_TS


def build_players_with_matches_ids_sql(
    *,
    limit: int = 50,
    days: int | None = 30,
) -> str:
    """
    Builds a SQL query to fetch teams, their ratings, and aggregated recent match IDs.

    The query joins teams with their ratings and recent matches, filtering by a
    minimum rating and a specified time window.

    Args:
        limit: Maximum number of teams to return.
        days: The number of days to look back for recent matches. If set to None,
              it filters matches since the latest patch timestamp.

    Returns:
        A formatted SQL query string.
    """
    if days is not None and days > 0:
        time_filter = f" m.start_time > EXTRACT(EPOCH FROM (NOW() - INTERVAL '{days} days'))::bigint"
    elif days is None:
        time_filter = f"m.start_time >= EXTRACT(EPOCH FROM TIMESTAMP '{LATEST_PATCH_TS}')"
    else:
        time_filter = "True"

    sql_template = """
                   WITH pro_player_matches AS (SELECT pm.account_id,
                                                      pm.match_id
                                               FROM player_matches pm
                                                        JOIN
                                                    matches m ON pm.match_id = m.match_id
                                                        JOIN
                                                    notable_players np ON pm.account_id = np.account_id
                                               WHERE np.is_pro = TRUE
                                                 AND {time_filter}),
                        player_match_aggregates AS (SELECT ppm.account_id,
                                                           STRING_AGG(ppm.match_id::varchar, ',' ORDER BY ppm.match_id DESC) AS match_ids
                                                    FROM pro_player_matches ppm
                                                    GROUP BY ppm.account_id)
                   SELECT np.account_id,
                          p.personaname,
                          np.name                     AS pro_name,
                          np.team_name,
                          np.team_id,
                          np.team_tag,
                          np.country_code,
                          p.steamid,
                          p.avatar,
                          p.avatarmedium,
                          p.avatarfull,
                          p.profileurl,
                          p.last_login,
                          p.full_history_time,
                          p.cheese,
                          p.fh_unavailable,
                          p.loccountrycode,
                          p.last_match_time,
                          p.plus,
                          np.fantasy_role,
                          np.team_id,
                          np.is_locked,
                          np.is_pro,
                          np.locked_until,
                          COALESCE(pma.match_ids, '') AS match_ids
                   FROM notable_players np
                            JOIN
                        players p ON np.account_id = p.account_id
                            LEFT JOIN
                        player_match_aggregates pma ON np.account_id = pma.account_id
                   WHERE np.is_pro = TRUE
                     AND np.is_locked = TRUE
                   ORDER BY np.team_id DESC
                   LIMIT {limit};
                   """

    query = textwrap.dedent(sql_template).format(
        time_filter=time_filter,
        limit=limit,
    )

    return query.strip()
