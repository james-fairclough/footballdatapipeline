{{ config(materialized='table') }}

with source as (
    select * from {{ source('staging', 'stg_player_stats') }}
),

player_stats as (
    select
        SAFE_CAST (SAFE_CAST(player_id AS FLOAT64) AS INT64) AS player_id,
        SAFE_CAST (SAFE_CAST(team_id AS FLOAT64) AS INT64) AS team_id,
        SAFE_CAST (SAFE_CAST(league_id AS FLOAT64) AS INT64) AS league_id,
        SAFE_CAST(league_season AS STRING) AS season,
        SAFE_CAST (SAFE_CAST(games_appearences AS FLOAT64) AS INT64) AS games_appearences,
        SAFE_CAST (SAFE_CAST(games_lineups AS FLOAT64) AS INT64) AS games_lineups,
        SAFE_CAST (SAFE_CAST(games_minutes AS FLOAT64) AS INT64) AS games_minutes,
        SAFE_CAST (SAFE_CAST(games_number AS FLOAT64) AS INT64) AS games_number,
        SAFE_CAST(games_position AS STRING) AS games_position,
        SAFE_CAST (SAFE_CAST(games_rating AS FLOAT64) AS DECIMAL) AS games_rating,
        SAFE_CAST(games_captain AS BOOLEAN) AS games_captain,
        SAFE_CAST (SAFE_CAST(substitutes_in AS FLOAT64) AS INT64) AS substitutes_in,
        SAFE_CAST (SAFE_CAST(substitutes_out AS FLOAT64) AS INT64) AS substitutes_out,
        SAFE_CAST (SAFE_CAST(substitutes_bench AS FLOAT64) AS INT64) AS substitutes_bench,
        SAFE_CAST (SAFE_CAST(shots_total AS FLOAT64) AS INT64) AS shots_total,
        SAFE_CAST (SAFE_CAST(shots_on AS FLOAT64) AS INT64) AS shots_on,
        SAFE_CAST (SAFE_CAST(goals_total AS FLOAT64) AS INT64) AS goals_total,
        SAFE_CAST (SAFE_CAST(goals_conceded AS FLOAT64) AS INT64) AS goals_conceded,
        SAFE_CAST (SAFE_CAST(goals_assists AS FLOAT64) AS INT64) AS goals_assists,
        SAFE_CAST (SAFE_CAST(goals_saves AS FLOAT64) AS INT64) AS goals_saves,
        SAFE_CAST (SAFE_CAST(passes_total AS FLOAT64) AS INT64) AS passes_total,
        SAFE_CAST (SAFE_CAST(passes_key AS FLOAT64) AS INT64) AS passes_key,
        SAFE_CAST (passes_accuracy AS FLOAT64) AS passes_accuracy,
        SAFE_CAST (SAFE_CAST(tackles_total AS FLOAT64) AS INT64) AS tackles_total,
        SAFE_CAST (SAFE_CAST(tackles_blocks AS FLOAT64) AS INT64) AS tackles_blocks,
        SAFE_CAST (SAFE_CAST(tackles_interceptions AS FLOAT64) AS INT64) AS tackles_interceptions,
        SAFE_CAST (SAFE_CAST(duels_total AS FLOAT64) AS INT64) AS duels_total,
        SAFE_CAST (SAFE_CAST(duels_won AS FLOAT64) AS INT64) AS duels_won,
        SAFE_CAST (SAFE_CAST(dribbles_attempts AS FLOAT64) AS INT64) AS dribbles_attempts,
        SAFE_CAST (SAFE_CAST(dribbles_success AS FLOAT64) AS INT64) AS dribbles_success,
        SAFE_CAST (SAFE_CAST(dribbles_past AS FLOAT64) AS INT64) AS dribbles_past,
        SAFE_CAST (SAFE_CAST(fouls_drawn AS FLOAT64) AS INT64) AS fouls_drawn,
        SAFE_CAST (SAFE_CAST(fouls_committed AS FLOAT64) AS INT64) AS fouls_committed,
        SAFE_CAST (SAFE_CAST(cards_yellow AS FLOAT64) AS INT64) AS cards_yellow,
        SAFE_CAST (SAFE_CAST(cards_yellowred AS FLOAT64) AS INT64) AS cards_yellowred,
        SAFE_CAST (SAFE_CAST(cards_red AS FLOAT64) AS INT64) AS cards_red,
        SAFE_CAST (SAFE_CAST(penalty_won AS FLOAT64) AS INT64) AS penalty_won,
        SAFE_CAST (SAFE_CAST(penalty_commited AS FLOAT64) AS INT64) AS penalty_committed,
        SAFE_CAST (SAFE_CAST(penalty_scored AS FLOAT64) AS INT64) AS penalty_scored,
        SAFE_CAST (SAFE_CAST(penalty_missed AS FLOAT64) AS INT64) AS penalty_missed,
        SAFE_CAST (SAFE_CAST(penalty_saved AS FLOAT64) AS INT64) AS penalty_saved

    from source where player_id is not null
)

select * from player_stats
