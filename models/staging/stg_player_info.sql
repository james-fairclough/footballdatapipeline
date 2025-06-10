{{ config(materialized='table') }}


WITH ranked_players AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY player_age desc) AS row_num
    FROM ({{ get_players_table() }}) 
)
SELECT * 
FROM ranked_players 
WHERE row_num = 1
