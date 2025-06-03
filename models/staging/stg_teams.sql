{{ config(materialized='table') }}

with source as (
    select * from {{ source('raw', 'Teams') }}
),

teams as (
    select
            cast(team_id as int) as team_id,
            team_name,
            team_code,
            team_country,
            safe_cast(cast(nullif(team_founded,"") as decimal) as int) as founded_year,
            team_national,
            team_logo,
            cast(cast(nullif(venue_id,"") as decimal)as int) as current_home_venue_id
        from source where team_id is not null
)


select * from teams 

