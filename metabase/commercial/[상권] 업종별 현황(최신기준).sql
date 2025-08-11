WITH
  top_rsb AS (
    SELECT
      fact_commercial_id
    FROM
      fact.fact_commercial
    WHERE
      TO_TIMESTAMP(time_key::TEXT, 'YYYYMMDDHH24MI') = (
        SELECT
          MAX(TO_TIMESTAMP(time_key::TEXT, 'YYYYMMDDHH24MI'))
        FROM
          fact.fact_commercial
        WHERE
          TO_TIMESTAMP(time_key::TEXT, 'YYYYMMDDHH24MI') <= GETDATE()
      )
    ORDER BY
      total_payment_count DESC
  )
SELECT
  c.category_large AS "대분류",
  c.category_medium AS "중분류",
  SUM(category_payment_count) AS "결제 건수(합계)",
  AVG(r.category_payment_max) AS "업종별 최대 결제 금액(평균)",
  TO_TIMESTAMP(r.time_key::TEXT, 'YYYYMMDDHH24MI') AS "기준 시각"
FROM
  fact.fact_commercial_rsb r
  JOIN dim.dim_category c ON r.category_id = c.category_id
WHERE
  r.fact_commercial_id IN (
    SELECT
      fact_commercial_id
    FROM
      top_rsb
  )
GROUP BY
  c.category_large,
  c.category_medium, r.time_key
ORDER BY
  SUM(category_payment_count) DESC;