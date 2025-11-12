{{ config(
    schema='silver',
    materialized='table'
) }}

WITH official_suburb AS (
    SELECT
        LOWER(TRIM(s.suburb_name)) AS suburb_name,
        TRIM(l.lga_code) AS lga_code,
        LOWER(TRIM(l.lga_name)) AS lga_name
    FROM {{ source('bronze', 'lga_suburb') }} s
    LEFT JOIN {{ source('bronze', 'lga_code') }} l
        ON LOWER(TRIM(s.lga_name)) = LOWER(TRIM(l.lga_name))
    WHERE s.suburb_name IS NOT NULL
),

listing_neigh AS (
    SELECT DISTINCT
        LOWER(TRIM(listing_neighbourhood)) AS listing_neighbourhood,
        listing_id
    FROM {{ ref('listings_clean') }}
)

SELECT
    COALESCE(l.listing_neighbourhood, o.suburb_name) AS listing_neighbourhood,
    o.suburb_name,
    o.lga_name,
    o.lga_code,
    md5(COALESCE(l.listing_neighbourhood, o.suburb_name)) AS suburb_key
FROM listing_neigh l
FULL OUTER JOIN official_suburb o
    ON l.listing_neighbourhood = o.suburb_name
ORDER BY listing_neighbourhood
