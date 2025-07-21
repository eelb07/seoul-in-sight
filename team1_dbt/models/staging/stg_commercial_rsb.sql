
{{ config(
    materialized='view',
    schema='staging'
) }}

SELECT
    scr.commercial_id,
    scr.source_id,
    comm.time_key, -- 조인된 commercial 테이블에서 time_key 추가
    scr.category_large,
    scr.category_medium,
    scr.category_congestion_level,
    scr.category_payment_count,
    scr.category_payment_min,
    scr.category_payment_max,
    scr.merchant_count,
    scr.merchant_basis_month,
    CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' AS created_at
FROM {{ source('raw_commercial_data', 'commercial_rsb') }} scr
LEFT JOIN (
    SELECT
        source_id,
        TO_CHAR(observed_at, 'YYYYMMDDHH24MI')::BIGINT AS time_key
    FROM {{ source('raw_commercial_data', 'commercial') }}
) comm
ON scr.source_id = comm.source_id