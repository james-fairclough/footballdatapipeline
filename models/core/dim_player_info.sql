{{ config(materialized='table') }}

with source as (
    select * from {{ source('staging', 'stg_player_info') }}
),

player_stats as (
    select
        SAFE_CAST(player_id AS INT64) AS player_id,
        SAFE_CAST(player_name AS STRING) AS player_name,
        SAFE_CAST(player_firstname AS STRING) AS player_firstname,
        SAFE_CAST(player_lastname AS STRING) AS player_lastname,
        SAFE_CAST(player_age AS INT64) AS player_age,
        SAFE_CAST(player_birth_date AS Date) AS player_birth_date,
        SAFE_CAST(player_birth_place AS STRING) AS player_birth_place,
        SAFE_CAST(player_birth_country AS STRING) AS player_birth_country,
        SAFE_CAST(player_nationality AS STRING) AS player_nationality,
        SAFE_CAST(player_height AS STRING) AS player_height,
        SAFE_CAST(player_weight AS STRING) AS player_weight,
        SAFE_CAST(player_injured AS BOOLEAN) AS player_injured,
        SAFE_CAST(player_photo AS STRING) AS player_photo

    from source where player_id is not null
)

select * from player_stats
