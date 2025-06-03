{{ config(materialized='table') }}

with source as (
    select * from {{ source('staging', 'stg_player_stats') }}
),

player_stats as (
    select
        Safe_CAST(player_id AS int64) AS player_id,
        Safe_CAST(team_id AS int64) AS team_id,
        Safe_CAST(league_id AS int64) AS league_id,
        Safe_CAST(league_season AS STRING) AS season,
        Safe_CAST(games_appearences AS int64) AS games_appearences,
        Safe_CAST(games_lineups AS int64) AS games_lineups,
        Safe_CAST(games_minutes AS int64) AS games_minutes,
        Safe_CAST(games_number AS int64) AS games_number,
        Safe_CAST(games_position AS STRING) AS games_position,
        Safe_CAST(games_rating AS decimal) AS games_rating,
        Safe_CAST(games_captain AS BOOLEAN) AS games_captain,
        Safe_CAST(substitutes_in AS int64) AS substitutes_in,
        Safe_CAST(substitutes_out AS int64) AS substitutes_out,
        Safe_CAST(substitutes_bench AS int64) AS substitutes_bench,
        Safe_CAST(shots_total AS int64) AS shots_total,
        Safe_CAST(shots_on AS int64) AS shots_on,
        Safe_CAST(goals_total AS int64) AS goals_total,
        Safe_CAST(goals_conceded AS int64) AS goals_conceded,
        Safe_CAST(goals_assists AS int64) AS goals_assists,
        Safe_CAST(goals_saves AS int64) AS goals_saves,
        Safe_CAST(passes_total AS int64) AS passes_total,
        Safe_CAST(passes_key AS int64) AS passes_key,
        Safe_CAST(passes_accuracy AS decimal) AS passes_accuracy,
        Safe_CAST(tackles_total AS int64) AS tackles_total,
        Safe_CAST(tackles_blocks AS int64) AS tackles_blocks,
        Safe_CAST(tackles_interceptions AS int64) AS tackles_int64erceptions,
        Safe_CAST(duels_total AS int64) AS duels_total,
        Safe_CAST(duels_won AS int64) AS duels_won,
        Safe_CAST(dribbles_attempts AS int64) AS dribbles_attempts,
        Safe_CAST(dribbles_success AS int64) AS dribbles_success,
        Safe_CAST(dribbles_past AS int64) AS dribbles_past,
        Safe_CAST(fouls_drawn AS int64) AS fouls_drawn,
        Safe_CAST(fouls_committed AS int64) AS fouls_committed,
        Safe_CAST(cards_yellow AS int64) AS cards_yellow,
        Safe_CAST(cards_yellowred AS int64) AS cards_yellowred,
        Safe_CAST(cards_red AS int64) AS cards_red,
        Safe_CAST(penalty_won AS int64) AS penalty_won,
        Safe_CAST(penalty_commited AS int64) AS penalty_commited,
        Safe_CAST(penalty_scored AS int64) AS penalty_scored,
        Safe_CAST(penalty_missed AS int64) AS penalty_missed,
        Safe_CAST(penalty_saved AS int64) AS penalty_saved
    from source where player_id is not null
)

select * from player_stats
