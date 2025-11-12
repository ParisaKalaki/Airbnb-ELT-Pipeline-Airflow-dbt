{{ config(
    schema='gold',
    materialized='table'
) }}

SELECT
    lga_code,
    age_0_4,
    age_5_14,
    age_15_19,
    age_20_24,
    age_25_34,
    age_35_44,
    age_45_54,
    age_55_64,
    age_65_74,
    age_75_84,
    age_85_plus,
    total_population,
    indigenous_population,
    born_overseas_population
FROM {{ ref('census_g01_clean') }}
