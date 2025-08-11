SELECT
  AR.AREA_NAME,
  DT.DATE_VALUE,
  DT.DAY_NAME,
  DT.is_weekend,
  AVG(POPULATION_MAX) AS "유동 인구",
  AVG(male_POPULATION_ratio) AS "남성(%)",
  AVG(female_POPULATION_ratio) AS "여성(%)",
  AVG(age_10s_ratio) AS "10대 이하(%)",
  AVG(age_20s_ratio) AS "20대(%)",
  AVG(age_30s_ratio) AS "30대(%)",
  AVG(age_40s_ratio) AS "40대(%)",
  AVG(age_50s_ratio) AS "50대(%)",
  AVG(age_60s_ratio) AS "60대 이상(%)",
  AVG(RESIDENT_RATIO) AS "상주",
  AVG(NON_RESIDENT_RATIO) AS "비상주"
FROM
  FACT.FACT_POPULATION C
  JOIN DIM.DIM_TIME DT ON C.TIME_KEY = DT.TIME_KEY
  JOIN DIM.DIM_AREA AR ON C.AREA_ID = AR.AREA_ID
GROUP BY
  AR.AREA_NAME,
  DT.DATE_VALUE,
  DT.DAY_NAME,
  DT.is_weekend
ORDER BY
  DT.DATE_VALUE,
  AR.area_name