SELECT
  AR.AREA_NAME,
  DT.DATE_VALUE,
  DT.DAY_NAME,
  DT.is_weekend,
  AVG(total_payment_count) AS "결제 건수(신한)",
  AVG(payment_amount_min) AS "결제 금액(최소)",
  AVG(payment_amount_max) AS "결제 금액(최대)",
  AVG(male_ratio) AS "남성(%)",
  AVG(female_ratio) AS "여성(%)",
  AVG(age_10s_ratio) AS "10대 이하(%)",
  AVG(age_20s_ratio) AS "20대(%)",
  AVG(age_30s_ratio) AS "30대(%)",
  AVG(age_40s_ratio) AS "40대(%)",
  AVG(age_50s_ratio) AS "50대(%)",
  AVG(age_60s_ratio) AS "60대 이상(%)",
  AVG(individual_consumer_ratio) AS "개인(%)",
  AVG(corporate_consumer_ratio) AS "법인(%)"
FROM
  FACT.FACT_COMMERCIAL C
  JOIN DIM.DIM_TIME DT ON C.TIME_KEY = DT.TIME_KEY
  JOIN DIM.DIM_AREA AR ON C.AREA_ID = AR.AREA_ID
GROUP BY
  AR.AREA_NAME,
  DT.DATE_VALUE,
  DT.DAY_NAME,
  DT.is_weekend
ORDER BY DT.DATE_VALUE, AR.area_name
  
  ;