WITH
  week_range AS (
    SELECT
      DATE_TRUNC('week', GETDATE()) - INTERVAL '7 days' AS start_of_last_week,
      DATE_TRUNC('week', GETDATE()) - INTERVAL '1 day' AS end_of_last_week
  )
SELECT
  a.area_name AS "장소명",
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
  fact.fact_commercial c
  JOIN dim.dim_area a ON c.area_id = a.area_id,
  week_range wr
WHERE
  TO_TIMESTAMP(c.time_key::TEXT, 'YYYYMMDDHH24MI') BETWEEN wr.start_of_last_week AND wr.end_of_last_week
GROUP BY
  a.area_name
ORDER BY
  avg(male_ratio) DESC
LIMIT
  10;