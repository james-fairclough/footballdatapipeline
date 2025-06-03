{{ config(materialized='table') }}

with source as (
    select * from {{ source('raw', 'Leagues') }}
),

unique_leagues as (
    select
        *
    from source where league_id is not null
)

select * from unique_leagues

