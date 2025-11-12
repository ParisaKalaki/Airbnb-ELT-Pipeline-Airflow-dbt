{{ config(schema='datamart', materialized='view') }}

WITH base AS (
    SELECT
        COALESCE(f.lga_key, 'Unknown') AS host_neighbourhood_lga,
        EXTRACT(YEAR FROM f.scraped_date) AS year,
        EXTRACT(MONTH FROM f.scraped_date) AS month,
        f.host_key,
        (30 - f.availability_30) * f.price AS estimated_revenue
    FROM {{ ref('fact_listings') }} f
    LEFT JOIN {{ ref('dm_host') }} h
        ON f.host_key = h.host_key
       AND f.scraped_date BETWEEN h.dbt_valid_from AND COALESCE(h.dbt_valid_to, CURRENT_DATE)
),

agg AS (
    SELECT
        host_neighbourhood_lga,
        year,
        month,
        COUNT(DISTINCT host_key) AS distinct_hosts,
        SUM(estimated_revenue) AS total_estimated_revenue,
        SUM(estimated_revenue) / NULLIF(COUNT(DISTINCT host_key), 0) AS estimated_revenue_per_host
    FROM base
    GROUP BY host_neighbourhood_lga, year, month
)

SELECT *
FROM agg
ORDER BY host_neighbourhood_lga, year, month
