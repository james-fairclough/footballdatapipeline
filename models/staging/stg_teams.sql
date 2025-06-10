{{ config(materialized='table') }}


SELECT * FROM ({{ get_teams_table() }})

