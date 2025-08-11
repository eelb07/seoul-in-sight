WITH week_range AS (
  SELECT
    DATE_TRUNC('week', GETDATE()) - INTERVAL '7 days' AS start_of_last_week,
    DATE_TRUNC('week', GETDATE()) - INTERVAL '1 day' AS end_of_last_week
)
SELECT
  a.area_name AS "장소명",
  AVG(c.total_payment_count) AS "결제 건수(신한)",
  AVG(c.age_10s_ratio) AS "10대 이하(%)",
  AVG(c.age_20s_ratio) AS "20대(%)",
  AVG(c.age_30s_ratio) AS "30대(%)",
  AVG(c.age_40s_ratio) AS "40대(%)",
  AVG(c.age_50s_ratio) AS "50대(%)",
  AVG(c.age_60s_ratio) AS "60대 이상(%)",
  AVG(c.individual_consumer_ratio) AS "개인(%)",
  AVG(c.corporate_consumer_ratio) AS "법인(%)",
  AVG(c.male_ratio) AS "남성(%)",
  AVG(c.female_ratio) AS "여성(%)"
FROM
  fact.fact_commercial c
JOIN
  dim.dim_area a ON c.area_id = a.area_id,
  week_range wr
WHERE
  TO_TIMESTAMP(c.time_key::TEXT, 'YYYYMMDDHH24MI')
  BETWEEN wr.start_of_last_week AND wr.end_of_last_week
GROUP BY
  a.area_id,
  a.area_name
ORDER BY
  a.area_name;
