{{ config(materialized='table') }}


SELECT DISTINCT *
FROM ({{ get_players_table() }}) 

