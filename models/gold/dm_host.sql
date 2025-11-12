{{ config(schema='gold', materialized='table') }}

SELECT
    host_id,
    md5(cast(host_id AS text)) AS host_key,
    host_name,
    host_is_superhost::boolean,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('host_snapshot') }}
