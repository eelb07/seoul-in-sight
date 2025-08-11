SELECT
  -- source_id, 
  category_large AS "대분류",
  category_medium AS "중분류",
  category_congestion_level AS "상권 현황",
  category_payment_count AS "업종별 결제 건수",
  category_payment_min AS "결제 금액(최소)",
  category_payment_max AS "결제 금액(최대)",
  merchant_count AS "가맹점 수",
  merchant_basis_month AS "가맹점 수 업데이트 기준",
  observed_at AS "기준 시각",
  created_at AS "적재 시간"
FROM
  source.source_commercial_rsb
ORDER BY
  category_large, observed_at
LIMIT
  10;