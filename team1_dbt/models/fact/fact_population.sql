{{ config(
    materialized='incremental',
    unique_key=['fact_population_id'],
    incremental_strategy='merge',
    schema='fact'
) }}

WITH stg_population AS (

    SELECT
	source_id,
        area_code,
        area_name,
        congestion_label,
        population_min,
        population_max,
        male_population_ratio,
        female_population_ratio,
        age_0s_ratio,
        age_10s_ratio,
        age_20s_ratio,
        age_30s_ratio,
        age_40s_ratio,
        age_50s_ratio,
        age_60s_ratio,
        age_70s_ratio,
        resident_ratio,
        non_resident_ratio,
        is_replaced,
        observed_at,
        created_at
    FROM {{ ref('stg_population') }}

    {% if is_incremental() %}
	WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}

),


final_fact AS (

    SELECT
	source_id AS fact_population_id,
        population_min,
        population_max,
        male_population_ratio,
        female_population_ratio,
        age_0s_ratio,
        age_10s_ratio,
        age_20s_ratio,
        age_30s_ratio,
        age_40s_ratio,
        age_50s_ratio,
        age_60s_ratio,
        age_70s_ratio,
        resident_ratio,
        non_resident_ratio,
        is_replaced,
        CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' AS created_at,
        CAST(da.area_id AS INT4) AS area_id,
        dc.congestion_id AS congestion_id,
        TO_CHAR(observed_at, 'YYYYMMDDHH24MI')::BIGINT AS time_key

    FROM stg_population sp
    LEFT JOIN dim.dim_congestion dc
        ON sp.congestion_label = dc.congestion_label
    LEFT JOIN dim.dim_area da
	ON sp.area_code = da.area_code
)

SELECT * FROM final_fact
