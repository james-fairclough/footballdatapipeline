{{ config(materialized='table') }}

with source as (
    select * from {{ source('staging', 'stg_teams') }}
),

teams as (
    select
        SAFE_CAST(team_id AS INT64) AS team_id,
        SAFE_CAST(team_name AS STRING) AS team_name,
        SAFE_CAST(team_code AS STRING) AS team_code,
        SAFE_CAST(team_country AS STRING) AS team_country,
        SAFE_CAST(team_founded AS STRING) AS team_founded,
        SAFE_CAST(team_national AS STRING) AS team_national,
        SAFE_CAST(team_logo AS STRING) AS team_logo,
        SAFE_CAST(venue_id AS INT64) AS venue_id,
        SAFE_CAST(venue_name AS STRING) AS venue_name,
        SAFE_CAST(venue_address AS STRING) AS venue_address,
        SAFE_CAST(venue_city AS STRING) AS venue_city,
        SAFE_CAST(venue_capacity AS INT64) AS venue_capacity,
        SAFE_CAST(venue_surface AS STRING) AS venue_surface,
        SAFE_CAST(venue_image AS STRING) AS venue_image


    from source where team_id is not null
)

select * from teams
