{{ config(schema='silver', materialized='table') }}

SELECT DISTINCT
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    host_neighbourhood
FROM {{ ref('listings_clean') }}
WHERE host_id IS NOT NULL
