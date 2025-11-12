{{ config(schema='gold', materialized='table') }}

SELECT DISTINCT
    md5(listing_id::text) AS property_key,
    listing_id,
    property_type,
    room_type,
    accommodates,
    number_of_reviews,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_value
FROM {{ ref('property_clean') }}
