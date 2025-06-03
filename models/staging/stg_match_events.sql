{{ config(materialized='table') }}

with source as (
    select * from {{ source('raw', 'Match_Events_PL_2023') }}
),

unique_match_events as (
    select
        *
    from source where fixture_id is not null
)

select * from unique_match_events

