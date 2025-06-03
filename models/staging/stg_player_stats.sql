{{ config(materialized='table') }}


SELECT * FROM ({{ get_players_stats_table() }})

