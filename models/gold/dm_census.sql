{{ config(
    schema='gold',
    materialized='table'
) }}

SELECT
    g01.lga_code,

    -- G01 demographic columns
    g01.age_0_4,
    g01.age_5_14,
    g01.age_15_19,
    g01.age_20_24,
    g01.age_25_34,
    g01.age_35_44,
    g01.age_45_54,
    g01.age_55_64,
    g01.age_65_74,
    g01.age_75_84,
    g01.age_85_plus,
    g01.total_population,
    g01.indigenous_population,
    g01.born_overseas_population,

    -- G02 household/economic columns
    g02.median_age_persons,
    g02.median_mortgage_repay_monthly,
    g02.median_tot_prsnl_inc_weekly,
    g02.median_rent_weekly,
    g02.median_tot_fam_inc_weekly,
    g02.average_num_psns_per_bedroom,
    g02.median_tot_hhd_inc_weekly,
    g02.average_household_size

FROM {{ ref('dm_census_g01') }} g01
LEFT JOIN {{ ref('dm_census_g02') }} g02
    ON g01.lga_code = g02.lga_code
