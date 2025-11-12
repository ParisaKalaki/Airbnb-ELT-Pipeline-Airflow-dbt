{{ config(schema='datamart', materialized='view') }}

WITH base AS (
    SELECT
        COALESCE(f.lga_key, 'Unknown') AS listing_neighbourhood_lga,
        EXTRACT(YEAR FROM f.scraped_date) AS year,
        EXTRACT(MONTH FROM f.scraped_date) AS month,
        f.listing_id,
        f.host_key,
        f.price,
        f.availability_30,
        f.number_of_reviews,
        f.property_key,
        h.host_is_superhost,
        l.accommodates,
        l.room_type,
        l.property_type,
        l.review_scores_rating,
        (30 - f.availability_30) AS number_of_stays,
        (30 - f.availability_30) * f.price AS estimated_revenue
    FROM {{ ref('fact_listings') }} f
    LEFT JOIN {{ ref('dm_host') }} h
        ON f.host_key = h.host_key
       AND f.scraped_date BETWEEN h.dbt_valid_from AND COALESCE(h.dbt_valid_to, CURRENT_DATE)
    LEFT JOIN {{ ref('dm_property') }} l
        ON f.property_key = l.property_key
),

agg AS (
    SELECT
        listing_neighbourhood_lga,
        year,
        month,
        COUNT(*) AS total_listings,
        COUNT(CASE WHEN availability_30 > 0 THEN 1 END) AS active_listings,
        MIN(CASE WHEN availability_30 > 0 THEN price END) AS min_price,
        MAX(CASE WHEN availability_30 > 0 THEN price END) AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN availability_30 > 0 THEN price END) AS median_price,
        AVG(CASE WHEN availability_30 > 0 THEN price END) AS avg_price,
        COUNT(DISTINCT host_key) AS distinct_hosts,
        COUNT(DISTINCT CASE WHEN availability_30 > 0 AND host_is_superhost THEN host_key END) * 100.0 / NULLIF(COUNT(DISTINCT host_key),0) AS superhost_rate,
        AVG(CASE WHEN availability_30 > 0 THEN review_scores_rating END) AS avg_review_score,
        SUM(CASE WHEN availability_30 > 0 THEN number_of_stays END) AS total_stays,
        SUM(CASE WHEN availability_30 > 0 THEN estimated_revenue END) AS total_estimated_revenue,
        SUM(CASE WHEN availability_30 > 0 THEN estimated_revenue END) / NULLIF(COUNT(CASE WHEN availability_30 > 0 THEN 1 END),0) AS avg_estimated_revenue_per_listing
    FROM base
    GROUP BY listing_neighbourhood_lga, year, month
),

pct_change AS (
    SELECT
        *,
        LAG(active_listings) OVER (PARTITION BY listing_neighbourhood_lga ORDER BY year, month) AS prev_active,
        LAG(total_listings - active_listings) OVER (PARTITION BY listing_neighbourhood_lga ORDER BY year, month) AS prev_inactive
    FROM agg
)

SELECT
    listing_neighbourhood_lga,
    year,
    month,
    active_listings * 100.0 / NULLIF(total_listings,0) AS active_listings_rate,
    min_price,
    max_price,
    median_price,
    avg_price,
    distinct_hosts,
    superhost_rate,
    avg_review_score,
    total_stays,
    (active_listings - prev_active) * 100.0 / NULLIF(prev_active,0) AS pct_change_active,
    ((total_listings - active_listings) - prev_inactive) * 100.0 / NULLIF(prev_inactive,0) AS pct_change_inactive,
    total_estimated_revenue,
    avg_estimated_revenue_per_listing
FROM pct_change
ORDER BY listing_neighbourhood_lga, year, month
