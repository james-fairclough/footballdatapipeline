{{ config(materialized='table') }}

with source as (
    select * from {{ source('staging', 'stg_leagues') }}
),

leagues as (
    select
        SAFE_CAST(seasons AS STRING) AS seasons,
        SAFE_CAST(league_id AS INT64) AS league_id,
        SAFE_CAST(league_name AS STRING) AS league_name,
        SAFE_CAST(league_type AS STRING) AS league_type,
        SAFE_CAST(league_logo AS STRING) AS league_logo,
        SAFE_CAST(country_name AS STRING) AS country_name,
        SAFE_CAST(country_code AS STRING) AS country_code,
        SAFE_CAST(country_flag AS STRING) AS country_flag

    from source where league_id is not null
)

select * from leagues
