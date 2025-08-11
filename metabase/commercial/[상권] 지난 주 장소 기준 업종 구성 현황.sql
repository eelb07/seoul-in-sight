WITH
  week_range AS (
    SELECT
      DATE_TRUNC('week', GETDATE()) - INTERVAL '7 days' AS start_of_last_week,
      DATE_TRUNC('week', GETDATE()) - INTERVAL '1 day' AS end_of_last_week
  )
SELECT
  a.area_name,
  ca.category_large AS "대분류",
  ca.category_medium AS "중분류",
  SUM(r.category_payment_count) AS "결제 건수(합계)",
  AVG(r.category_payment_max) AS "업종별 최대 결제 금액(평균)"
FROM
  fact.fact_commercial_rsb r
  JOIN fact.fact_commercial c ON r.fact_commercial_id = c.fact_commercial_id
  JOIN dim.dim_category ca ON r.category_id = ca.category_id
  JOIN week_range wr ON TO_TIMESTAMP(c.time_key::TEXT, 'YYYYMMDDHH24MI') BETWEEN wr.start_of_last_week AND wr.end_of_last_week
  JOIN dim.dim_area a ON c.area_id = a.area_id
GROUP BY
  a.area_name,
  ca.category_large,
  ca.category_medium
ORDER BY
  a.area_name,
  SUM(r.category_payment_count) DESC;