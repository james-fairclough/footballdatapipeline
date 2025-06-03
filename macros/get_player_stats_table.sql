{% macro get_players_stats_table() %}
    {% set query %}
        SELECT table_name 
        FROM `footballdataproject-458811.PlayerComparisons.INFORMATION_SCHEMA.TABLES`
        WHERE table_name LIKE 'Players_stats_%'
    {% endset %}
    
    {% set results = run_query(query) %}
    
    {% for row in results %}
        SELECT * FROM `footballdataproject-458811.PlayerComparisons.{{ row['table_name'] }}` {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
{% endmacro %}
