{{ config(materialized='table') }}

with source as (
    select * from {{ source('raw', 'Fixtures_PL_2023') }}
),

unique_fixtures as (
    select
        *
    from source where fixture_id is not null
)

select * from unique_fixtures

