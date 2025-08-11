SELECT
  a.area_name AS "장소명",
  a.category AS "카테고리명",
  c.congestion_level AS "상권 현황",
  c.total_payment_count AS "결제 건수(신한)",
  c.payment_amount_max AS "결제 금액(최대)",
  TO_TIMESTAMP(c.time_key::TEXT, 'YYYYMMDDHH24MI') AS "기준 시각"
FROM
  fact.fact_commercial c
  JOIN dim.dim_area a ON c.area_id = a.area_id
WHERE
  TO_TIMESTAMP(time_key::TEXT, 'YYYYMMDDHH24MI') = (
    SELECT
      MAX(TO_TIMESTAMP(time_key::TEXT, 'YYYYMMDDHH24MI'))
    FROM
      fact.fact_commercial
    WHERE
      TO_TIMESTAMP(time_key::TEXT, 'YYYYMMDDHH24MI') <= GETDATE()
  ) -- 현재 시점과 가장 가까운 기준 시각 하나를 잡아 해당 시각을 기준으로 비교 
ORDER BY
  payment_amount_max DESC
LIMIT
  10;