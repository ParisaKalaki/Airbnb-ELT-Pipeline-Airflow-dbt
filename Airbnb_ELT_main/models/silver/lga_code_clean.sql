{{ config(schema='silver', materialized='table') }}

SELECT DISTINCT
    TRIM(lga_code) AS lga_code,
    LOWER(TRIM(lga_name)) AS lga_name
FROM {{ source('bronze', 'lga_code') }}
WHERE lga_code IS NOT NULL

