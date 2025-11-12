{{ config(schema='gold', materialized='table') }}

WITH mapped AS (
    SELECT
        f.*,
        s.suburb_key,
        l.lga_key,
        l.lga_code,
        p.property_key,
        h.host_key
    FROM {{ ref('listings_clean') }} f

    LEFT JOIN {{ ref('dm_suburb') }} s
        ON LOWER(TRIM(f.listing_neighbourhood)) = LOWER(TRIM(s.suburb_name))

    LEFT JOIN {{ ref('dm_lga') }} l
        ON s.lga_code = l.lga_code

    LEFT JOIN {{ ref('dm_property') }} p
        ON f.listing_id = p.listing_id

    LEFT JOIN {{ ref('dm_host') }} h
        ON f.host_id = h.host_id
)

SELECT
    listing_id,
    scraped_date,
    price,
    number_of_reviews,
    availability_30,
    host_key,
    suburb_key,
    lga_key,
    lga_code,
    property_key
FROM mapped

