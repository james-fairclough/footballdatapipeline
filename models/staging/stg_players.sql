{{ config(materialized='table') }}

with source as (
    select * from {{ source('raw', 'Players_PL_2023') }}
),

unique_players as (
    select
        *
    from source where player_id is not null
)

select * from unique_players

