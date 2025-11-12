-------------------------------------------------------- Question a
-- Step 1️⃣: Compute total revenue per host per LGA
WITH host_revenue AS (
    SELECT
        f.lga_code,
        f.host_key,
        AVG((30 - f.availability_30) * f.price) AS revenue_per_host
    FROM gold.fact_listings f
    WHERE f.scraped_date >= (
        SELECT MAX(scraped_date) - INTERVAL '12 months' FROM gold.fact_listings
    )
    AND f.availability_30 < 30
    GROUP BY f.lga_code, f.host_key
),

-- Step 2️⃣: Average revenue across hosts per LGA
revenue_last_12_months AS (
    SELECT
        lga_code,
        AVG(revenue_per_host) AS avg_revenue_per_host
    FROM host_revenue
    GROUP BY lga_code
),

-- Step 3️⃣: Rank LGAs
ranked_lgas AS (
    SELECT
        lga_code,
        avg_revenue_per_host,
        DENSE_RANK() OVER (ORDER BY avg_revenue_per_host DESC) AS rank_top,
        DENSE_RANK() OVER (ORDER BY avg_revenue_per_host ASC) AS rank_bottom
    FROM revenue_last_12_months
),

-- Step 4️⃣: Keep Top 3 and Bottom 3
top_bottom_lgas AS (
    SELECT
        lga_code,
        CASE
            WHEN rank_top <= 3 THEN 'Top 3'
            WHEN rank_bottom <= 3 THEN 'Bottom 3'
        END AS performance_group
    FROM ranked_lgas
    WHERE rank_top <= 3 OR rank_bottom <= 3
),

-- Step 5️⃣: Join demographics
demographics AS (
    SELECT
        t.performance_group,
        t.lga_code,
        c.total_population,
        (c.age_0_4 + c.age_5_14) AS age_0_14,
        (c.age_15_19 + c.age_20_24 + c.age_25_34 + c.age_35_44 + c.age_45_54 + c.age_55_64) AS age_15_64,
        (c.age_65_74 + c.age_75_84 + c.age_85_plus) AS age_65_plus,
        c.average_household_size
    FROM top_bottom_lgas t
    LEFT JOIN gold.dm_census c
        ON 'LGA' || t.lga_code::text = c.lga_code
)

-- Step 6️⃣: Aggregate by performance group
SELECT
    performance_group,
    ROUND(SUM(age_0_14) * 100.0 / NULLIF(SUM(total_population), 0), 2) AS pct_age_0_14,
    ROUND(SUM(age_15_64) * 100.0 / NULLIF(SUM(total_population), 0), 2) AS pct_age_15_64,
    ROUND(SUM(age_65_plus) * 100.0 / NULLIF(SUM(total_population), 0), 2) AS pct_age_65_plus,
    ROUND(AVG(average_household_size), 2) AS avg_household_size,
    STRING_AGG(DISTINCT lga_code, ', ') AS lgas_in_group
FROM demographics
GROUP BY performance_group
ORDER BY performance_group DESC;
-------------------------------------------------------- Question b
WITH revenue_by_neighbourhood AS (
    SELECT
        lga_code,
        AVG((30 - availability_30) * price) AS avg_revenue_per_active_listing
    FROM gold.fact_listings
    WHERE availability_30 < 30
    GROUP BY lga_code
),

joined_data AS (
    SELECT
        r.lga_code,
        c.median_age_persons,
        r.avg_revenue_per_active_listing
    FROM revenue_by_neighbourhood r
    INNER JOIN gold.dm_census_g02 c ON 'LGA' || r.lga_code::text = c.lga_code
    WHERE c.median_age_persons IS NOT NULL
)

SELECT
    ROUND(CORR(
        joined_data.median_age_persons::numeric,
        joined_data.avg_revenue_per_active_listing::numeric
    )::numeric, 3) AS correlation_coefficient
FROM joined_data;
-------------------------------------------------------- Question c
-- Step 1️⃣: Use only the latest snapshot per listing
WITH latest_snapshot AS (
    SELECT DISTINCT ON (listing_id)
        f.listing_id,
        f.suburb_key,
        f.property_key,
        f.availability_30,
        f.price
    FROM gold.fact_listings f
    ORDER BY f.listing_id, f.scraped_date DESC
),

-- Step 2️⃣: Compute average revenue per active listing per neighbourhood
revenue_per_neighbourhood AS (
    SELECT
        s.suburb_name AS listing_neighbourhood,
        AVG((30 - l.availability_30) * l.price) AS avg_revenue_per_active_listing
    FROM latest_snapshot l
    LEFT JOIN gold.dm_suburb s
        ON l.suburb_key = s.suburb_key
    WHERE l.availability_30 < 30
    GROUP BY s.suburb_name
),

-- Step 3️⃣: Identify top 5 listing_neighbourhoods
top5_neighbourhoods AS (
    SELECT listing_neighbourhood
    FROM revenue_per_neighbourhood
    ORDER BY avg_revenue_per_active_listing DESC
    LIMIT 5
),

-- Step 4️⃣: Aggregate stays by property characteristics in top 5 neighbourhoods
stays_by_property AS (
    SELECT
        s.suburb_name AS listing_neighbourhood,
        p.property_type,
        p.room_type,
        p.accommodates,
        SUM(30 - l.availability_30) AS total_stays
    FROM latest_snapshot l
    LEFT JOIN gold.dm_property p
        ON l.property_key = p.property_key
    LEFT JOIN gold.dm_suburb s
        ON l.suburb_key = s.suburb_key
    WHERE s.suburb_name IN (SELECT listing_neighbourhood FROM top5_neighbourhoods)
    GROUP BY s.suburb_name, p.property_type, p.room_type, p.accommodates
),

-- Step 5️⃣: Identify the best combination per neighbourhood
best_property_per_neighbourhood AS (
    SELECT DISTINCT ON (listing_neighbourhood)
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        total_stays
    FROM stays_by_property
    ORDER BY listing_neighbourhood, total_stays DESC
)

-- Step 6️⃣: Return results
SELECT *
FROM best_property_per_neighbourhood
ORDER BY listing_neighbourhood;
-------------------------------------------------------- Question d
WITH host_listings AS (
    SELECT
        host_key,
        lga_code,
        listing_id,
        availability_30
    FROM gold.fact_listings
    WHERE has_availability = TRUE
      AND host_key IS NOT NULL
      AND lga_key IS NOT NULL
),
host_summary AS (
    SELECT
        host_key,
        COUNT(DISTINCT lga_code) AS num_lgas,
        COUNT(listing_id) AS total_listings
    FROM host_listings
    GROUP BY host_key
    HAVING COUNT(listing_id) > 1
)
SELECT
    COUNT(host_key) AS total_multi_listing_hosts,
    COUNT(CASE WHEN num_lgas = 1 THEN 1 END) AS concentrated_hosts_count,
    COUNT(CASE WHEN num_lgas > 1 THEN 1 END) AS distributed_hosts_count,
    ROUND(
        COUNT(CASE WHEN num_lgas = 1 THEN 1 END) * 100.0 / COUNT(host_key),
        2
    ) AS percent_concentrated_hosts
FROM host_summary;
-------------------------------------------------------- Question e
WITH single_listing_hosts AS (
    -- 1️⃣ Identify hosts with only one active listing over the last 12 months
    SELECT
        host_key,
        listing_id,
        lga_code,
        -- Annual estimated revenue for this single listing
        (30 - availability_30) * price * 12 AS annual_estimated_revenue
    FROM gold.fact_listings
    WHERE availability_30 > 0
      AND host_key IS NOT NULL
      AND lga_code IS NOT NULL
      AND scraped_date >= (SELECT MAX(scraped_date) - INTERVAL '12 months' FROM gold.fact_listings)
      AND host_key IN (
          SELECT host_key
          FROM gold.fact_listings
          WHERE availability_30 > 0
          GROUP BY host_key
          HAVING COUNT(DISTINCT listing_id) = 1
      )
),

mortgage_coverage AS (
    -- 2️⃣ Join with GOLD census data for median mortgage
    SELECT
        s.host_key,
        s.lga_code,
        s.annual_estimated_revenue,
        CAST(c.median_mortgage_repay_monthly AS NUMERIC) * 12 AS annual_median_mortgage
    FROM single_listing_hosts s
    INNER JOIN gold.dm_census_g02 c
        ON 'LGA' || s.lga_code::text = c.lga_code
),

lga_summary AS (
    -- 3️⃣ Calculate coverage counts per LGA
    SELECT
        lga_code,
        COUNT(host_key) AS total_single_hosts,
        COUNT(CASE WHEN annual_estimated_revenue >= annual_median_mortgage THEN 1 END) AS covered_hosts_count
    FROM mortgage_coverage
    GROUP BY lga_code
)

-- 4️⃣ Join with LGA names and find top-performing LGA
SELECT
    l.lga_name,
    s.lga_code,
    ROUND(s.covered_hosts_count * 100.0 / s.total_single_hosts, 2) AS percent_covered,
    s.total_single_hosts
FROM lga_summary s
LEFT JOIN gold.dm_lga l
    ON s.lga_code = l.lga_code
WHERE s.total_single_hosts > 10
ORDER BY percent_covered DESC, total_single_hosts DESC
LIMIT 1;


