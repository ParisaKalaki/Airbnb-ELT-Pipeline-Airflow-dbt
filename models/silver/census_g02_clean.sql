{{ config(schema='silver', materialized='table') }}

SELECT
    CAST(LGA_CODE_2016 AS TEXT) AS lga_code,

    Median_age_persons,
    Median_mortgage_repay_monthly,
    Median_tot_prsnl_inc_weekly,
    Median_rent_weekly,
    Median_tot_fam_inc_weekly,
    Average_num_psns_per_bedroom,
    Median_tot_hhd_inc_weekly,
    Average_household_size

FROM {{ source('bronze', 'census_g02_raw') }}
WHERE LGA_CODE_2016 IS NOT NULL