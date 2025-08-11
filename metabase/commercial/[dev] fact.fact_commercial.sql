SELECT
  -- fact_commercial_id AS 
  congestion_level AS "상권 현황",
  total_payment_count AS "결제 건수(신한)",
  payment_amount_min AS "결제 금액(최소)",
  payment_amount_max AS "결제 금액(최대)",
  male_ratio AS "남성(%)",
  female_ratio AS "여성(%)",
  age_10s_ratio AS "10대 이하(%)",
  age_20s_ratio AS "20대(%)",
  age_30s_ratio AS "30대(%)",
  age_40s_ratio AS "40대(%)",
  age_50s_ratio AS "50대(%)",
  age_60s_ratio AS "60대 이상(%)",
  individual_consumer_ratio AS "개인(%)",
  corporate_consumer_ratio AS "법인(%)",
  area_id AS "장소명(ID)",
  time_key AS "기준 시각" 
FROM
  fact.fact_commercial
ORDER BY
  area_id
LIMIT
  10;
;