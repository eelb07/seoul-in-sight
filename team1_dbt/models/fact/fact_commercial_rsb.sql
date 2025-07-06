{{ config(
    materialized='incremental',
    unique_key=['fact_commercial_rsb_id'], 
    incremental_strategy='merge',         
    schema='fact'                         
) }}

WITH stg_commercial_rsb_data AS (
    SELECT
        commercial_id,             
        source_id,        
        category_large,
        category_medium,
        category_congestion_level,
        category_payment_count,
        category_payment_min,  
        category_payment_max,  
        merchant_count,
        merchant_basis_month
    FROM {{ ref('stg_commercial_rsb') }}

    {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
),

final_fact AS (
    SELECT
        source_id AS fact_commercial_id,
        commercial_id AS fact_commercial_rsb_id, 
        category_congestion_level::VARCHAR(20) AS category_congestion_level,
        category_payment_count::INT AS category_payment_count,
        category_payment_min::INT AS category_payment_min, 
        category_payment_max::INT AS category_payment_max, 
        merchant_count::INT AS merchant_count,
        merchant_basis_month::CHARACTER(6) AS merchant_basis_month,
        NULL::SMALLINT AS category_id,  
        GETDATE() AS created_at
    FROM stg_commercial_rsb_data
)

SELECT * FROM final_fact