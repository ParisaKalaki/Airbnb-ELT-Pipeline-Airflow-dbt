{{ config(
    schema='silver',
    materialized='incremental',
    unique_key=['listing_id', 'scraped_date']
) }}

WITH base AS (
    SELECT
        listing_id,
        scrape_id,
        scraped_date,
        host_id,
        host_name,
        host_since,
        CASE 
            WHEN host_is_superhost = 't' THEN TRUE
            WHEN host_is_superhost = 'f' THEN FALSE
            ELSE NULL 
        END AS host_is_superhost,
        host_neighbourhood,
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates::integer AS accommodates,
        CAST(price AS numeric) AS price,
        CASE 
            WHEN has_availability = 't' THEN TRUE
            WHEN has_availability = 'f' THEN FALSE
            ELSE NULL 
        END AS has_availability,
        availability_30::integer AS availability_30,
        number_of_reviews::integer AS number_of_reviews,
        review_scores_rating::numeric AS review_scores_rating,
        review_scores_accuracy::numeric AS review_scores_accuracy,
        review_scores_cleanliness::numeric AS review_scores_cleanliness,
        review_scores_checkin::numeric AS review_scores_checkin,
        review_scores_communication::numeric AS review_scores_communication,
        review_scores_value::numeric AS review_scores_value
    FROM {{ source('bronze', 'airbnb_raw') }}

)

SELECT * FROM base
