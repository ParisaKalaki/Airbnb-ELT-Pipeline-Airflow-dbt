{% macro create_fact_listings_indexes() %}
  {# Macro to create indexes on fact_listings table #}

  {% set table_name = 'gold.fact_listings' %}

  -- Index for joins to dm_host
  CREATE INDEX IF NOT EXISTS idx_fact_listings_host_key 
    ON {{ table_name }} (host_key);

  -- Index for joins to dm_suburb
  CREATE INDEX IF NOT EXISTS idx_fact_listings_suburb_key 
    ON {{ table_name }} (suburb_key);

  -- Index for joins to dm_property
  CREATE INDEX IF NOT EXISTS idx_fact_listings_property_key 
    ON {{ table_name }} (property_key);

  -- Index for aggregation and date filtering
  CREATE INDEX IF NOT EXISTS idx_fact_listings_scraped_year_month
    ON {{ table_name }} (EXTRACT(YEAR FROM scraped_date), EXTRACT(MONTH FROM scraped_date));

{% endmacro %}
