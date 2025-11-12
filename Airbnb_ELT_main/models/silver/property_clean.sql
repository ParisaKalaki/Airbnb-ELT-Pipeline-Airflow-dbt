{{ config(schema='silver', materialized='table') }}

SELECT DISTINCT
    listing_id,
    property_type,
    room_type,
    accommodates,
    number_of_reviews::integer AS number_of_reviews,
    review_scores_rating::numeric AS review_scores_rating,
    review_scores_accuracy::numeric AS review_scores_accuracy,
    review_scores_cleanliness::numeric AS review_scores_cleanliness,
    review_scores_checkin::numeric AS review_scores_checkin,
    review_scores_communication::numeric AS review_scores_communication,
    review_scores_value::numeric AS review_scores_value
FROM {{ ref('listings_clean') }}
WHERE listing_id IS NOT NULL
