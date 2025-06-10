{{ config(materialized='table') }}


SELECT * FROM ({{ get_leagues_table() }})