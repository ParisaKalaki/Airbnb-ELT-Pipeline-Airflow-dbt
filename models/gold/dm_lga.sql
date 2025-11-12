{{ config(schema='gold', materialized='table') }}

SELECT
    md5(cast(lga_code as text)) as lga_key,
    lga_code,
    lga_name
FROM {{ ref('lga_code_clean') }}

