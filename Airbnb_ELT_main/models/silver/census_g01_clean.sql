{{ config(schema='silver', materialized='table') }}

SELECT
    CAST(LGA_CODE_2016 AS TEXT) AS lga_code,

    -- Total population per age group
    (Age_0_4_yr_M + Age_0_4_yr_F) AS age_0_4,
    (Age_5_14_yr_M + Age_5_14_yr_F) AS age_5_14,
    (Age_15_19_yr_M + Age_15_19_yr_F) AS age_15_19,
    (Age_20_24_yr_M + Age_20_24_yr_F) AS age_20_24,
    (Age_25_34_yr_M + Age_25_34_yr_F) AS age_25_34,
    (Age_35_44_yr_M + Age_35_44_yr_F) AS age_35_44,
    (Age_45_54_yr_M + Age_45_54_yr_F) AS age_45_54,
    (Age_55_64_yr_M + Age_55_64_yr_F) AS age_55_64,
    (Age_65_74_yr_M + Age_65_74_yr_F) AS age_65_74,
    (Age_75_84_yr_M + Age_75_84_yr_F) AS age_75_84,
    (Age_85ov_M + Age_85ov_F) AS age_85_plus,

    -- Total population
    Tot_P_P AS total_population,

    -- Indigenous and born overseas
    Indigenous_P_Tot_P AS indigenous_population,
    Birthplace_Elsewhere_P AS born_overseas_population

FROM {{ source('bronze', 'census_g01_raw') }}
WHERE LGA_CODE_2016 IS NOT NULL
