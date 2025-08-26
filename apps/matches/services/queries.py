"""
SQL query builders for fetching complex, filtered match data.
"""

from __future__ import annotations

import textwrap
from typing import TYPE_CHECKING

from apps.core.conf import LATEST_PATCH_TS

if TYPE_CHECKING:
    from apps.matches.conf import MatchFetcherConfig


def build_match_ids_query(config: MatchFetcherConfig) -> str:
    """
    Builds a dynamic SQL query to fetch a list of match IDs based on the config.
    """
    cte_parts = []
    where_conditions = ["TRUE"]
    joins = []

    if config.player_account_ids:
        player_ids_str = ", ".join(map(str, config.player_account_ids))
        cte_parts.append(
            textwrap.dedent(f"""
            matches_by_player AS (
                SELECT DISTINCT match_id FROM player_matches WHERE account_id IN ({player_ids_str})
            )
        """),
        )
        joins.append("JOIN matches_by_player mbp ON m.match_id = mbp.match_id")

    if config.hero_ids:
        hero_ids_str = ", ".join(map(str, config.hero_ids))
        cte_parts.append(
            textwrap.dedent(f"""
            matches_by_hero AS (
                SELECT DISTINCT match_id FROM player_matches WHERE hero_id IN ({hero_ids_str})
            )
        """),
        )
        joins.append("JOIN matches_by_hero mbh ON m.match_id = mbh.match_id")

    if config.team_ids:
        team_ids_str = ", ".join(map(str, config.team_ids))
        where_conditions.append(f"(m.radiant_team_id IN ({team_ids_str}) OR m.dire_team_id IN ({team_ids_str}))")

    if config.league_ids:
        league_ids_str = ", ".join(map(str, config.league_ids))
        where_conditions.append(f"m.leagueid IN ({league_ids_str})")

    if config.start_date:
        start_epoch = int(config.start_date.timestamp())
        where_conditions.append(f"m.start_time >= {start_epoch}")

    if config.end_date:
        end_epoch = int(config.end_date.timestamp())
        where_conditions.append(f"m.start_time <= {end_epoch}")

    if config.min_duration is not None:
        where_conditions.append(f"m.duration >= {config.min_duration}")

    if config.is_parsed is not None:
        if config.is_parsed:
            where_conditions.append(
                "EXISTS (SELECT 1 FROM parsed_matches pm WHERE pm.match_id = m.match_id AND pm.is_archived = TRUE)",
            )
        else:
            where_conditions.append(
                "NOT EXISTS (SELECT 1 FROM parsed_matches pm WHERE pm.match_id = m.match_id AND pm.is_archived = TRUE)",
            )

    cte_clause = "WITH " + ",\n".join(cte_parts) if cte_parts else ""
    where_clause = "\n    AND ".join(where_conditions)
    join_clause = "\n".join(joins)

    sql_template = """
        {cte_clause}
        SELECT m.match_id
        FROM matches AS m
        {join_clause}
        WHERE {where_clause}
        ORDER BY m.start_time DESC
        LIMIT {limit}
        OFFSET {offset};
    """
    return textwrap.dedent(sql_template).format(
        cte_clause=cte_clause,
        join_clause=join_clause,
        where_clause=where_clause,
        limit=config.limit,
        offset=config.offset,
    )


def build_full_matches_data_query(match_ids: list[int]) -> str:
    """
    Builds the comprehensive SQL query to fetch all structured data for a specific list of match IDs.
    """
    if not match_ids:
        return "SELECT * FROM matches WHERE FALSE;"

    match_ids_str = ", ".join(map(str, match_ids))

    sql_template = """
                   WITH player_data AS (SELECT pm.match_id,
                                               json_agg(json_build_object(
                                                            'account_id', pm.account_id,
                                                            'player_slot', pm.player_slot,
                                                            'hero_id', pm.hero_id,
                                                            'hero_name', h.localized_name,
                                                            'personaname', p.personaname,
                                                            'kills', pm.kills,
                                                            'deaths', pm.deaths,
                                                            'assists', pm.assists,
                                                            'net_worth', pm.net_worth,
                                                            'gold_per_min', pm.gold_per_min,
                                                            'xp_per_min', pm.xp_per_min,
                                                            'level', pm.level,
                                                            'items',
                                                            json_build_array(pm.item_0, pm.item_1, pm.item_2, pm.item_3,
                                                                             pm.item_4, pm.item_5),
                                                            'backpack',
                                                            json_build_array(pm.backpack_0, pm.backpack_1, pm.backpack_2),
                                                            'item_neutral', pm.item_neutral,
                                                            'hero_damage', pm.hero_damage,
                                                            'tower_damage', pm.tower_damage,
                                                            'purchase_log', pm.purchase_log
                                                        ) ORDER BY pm.player_slot ASC) AS players_json
                                        FROM player_matches AS pm
                                                 LEFT JOIN players p ON pm.account_id = p.account_id
                                                 LEFT JOIN heroes h ON pm.hero_id = h.id
                                        WHERE pm.match_id IN ({match_ids_str})
                                        GROUP BY pm.match_id),
                        draft_data AS (SELECT pb.match_id,
                                              json_agg(json_build_object(
                                                           'is_pick', pb.is_pick, 'hero_id', pb.hero_id, 'team',
                                                           pb.team,
                                                           'order', pb.ord, 'hero_name', h.localized_name
                                                       ) ORDER BY pb.ord ASC) AS draft_json
                                       FROM picks_bans pb
                                                LEFT JOIN heroes h ON pb.hero_id = h.id
                                       WHERE pb.match_id IN ({match_ids_str})
                                       GROUP BY pb.match_id)
                   SELECT m.*,
                          COALESCE(pd.players_json, '[]'::json) AS players,
                          COALESCE(dd.draft_json, '[]'::json)   AS draft,
                          l.name                                AS league_name,
                          mp.patch
                   FROM matches AS m
                            LEFT JOIN player_data pd ON m.match_id = pd.match_id
                            LEFT JOIN draft_data dd ON m.match_id = dd.match_id
                            LEFT JOIN leagues l ON m.leagueid = l.leagueid
                            LEFT JOIN match_patch mp ON m.match_id = mp.match_id
                   WHERE m.match_id IN ({match_ids_str})
                   ORDER BY m.start_time DESC; \
                   """
    return textwrap.dedent(sql_template).format(match_ids_str=match_ids_str)


def build_all_matches_last_n_days(days: int) -> str:
    """
    Builds the comprehensive SQL query to fetch all structured data for the last N days,
    or since the latest patch timestamp if _days is None.
    """
    if days is not None:
        time_filter = f"m.start_time >= EXTRACT(EPOCH FROM (NOW() - INTERVAL '{days} days'))"
    else:
        time_filter = f"m.start_time >= EXTRACT(EPOCH FROM TIMESTAMP '{LATEST_PATCH_TS}')"

    sql_template = """
                   SELECT STRING_AGG(m.match_id::TEXT, ',' ORDER BY m.start_time DESC) AS match_ids
                   FROM matches m
                   WHERE {time_filter}
                   LIMIT 200;\
                   """
    return textwrap.dedent(sql_template).format(time_filter=time_filter)
