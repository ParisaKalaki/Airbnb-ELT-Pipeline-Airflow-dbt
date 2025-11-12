{{ config(schema='gold', materialized='table') }}

SELECT DISTINCT
    md5(lower(trim(COALESCE(listing_neighbourhood, suburb_name)))) AS suburb_key,
    COALESCE(listing_neighbourhood, suburb_name) AS suburb_name,
    lga_code,
    lga_name
FROM {{ ref('lga_suburb_clean') }}
WHERE COALESCE(listing_neighbourhood, suburb_name) IS NOT NULL
