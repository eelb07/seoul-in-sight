SELECT
  area_code AS "장소명(코드)",
  area_name AS "장소명",
  congestion_level AS "상권 현황",
  total_payment_count AS "결제 건수(신한)",
  payment_amount_min AS "결제 금액(최소)",
  payment_amount_max AS "결제 금액(최대)",
  male_ratio AS "남성(%)", -- 변경: 남성 소비 비율 -> 남성(%)
  female_ratio AS "여성(%)", -- 변경: 여성 소비 비율 -> 여성(%)
  age_10s_ratio AS "10대 이하(%)", -- 변경: 10대 이하 소비 비율 -> 10대 이하(%)
  age_20s_ratio AS "20대(%)", -- 변경: 20대 소비 비율 -> 20대(%)
  age_30s_ratio AS "30대(%)", -- 변경: 30대 소비 비율 -> 30대(%)
  age_40s_ratio AS "40대(%)", -- 변경: 40대 소비 비율 -> 40대(%)
  age_50s_ratio AS "50대(%)", -- 변경: 50대 소비 비율 -> 50대(%)
  age_60s_ratio AS "60대 이상(%)", -- 변경: 60대 이상 소비 비율 -> 60대 이상(%)
  individual_consumer_ratio AS "개인(%)", -- 변경: 개인 소비 비율 -> 개인(%)
  corporate_consumer_ratio AS "법인(%)", -- 변경: 법인 소비 비율 -> 법인(%)
  observed_at AS "기준 시각",
  created_at AS "적재 시간"
FROM
  source.source_commercial
ORDER BY
  area_name
LIMIT
  10;