WITH
  week_range AS (
    SELECT
      DATE_TRUNC('week', GETDATE()) - INTERVAL '7 days' AS start_of_last_week,
      DATE_TRUNC('week', GETDATE()) - INTERVAL '1 day' AS end_of_last_week
  )
SELECT
  CASE EXTRACT(
      DOW
      FROM
        dt.datetime_value
    )
    WHEN 0 THEN '일요일'
    WHEN 1 THEN '월요일'
    WHEN 2 THEN '화요일'
    WHEN 3 THEN '수요일'
    WHEN 4 THEN '목요일'
    WHEN 5 THEN '금요일'
    WHEN 6 THEN '토요일'
  END AS "요일",
  ROUND(AVG(fw.temperature), 1) AS "평균 기온",
  ROUND(MAX(fw.max_temperature), 1) AS "최고 기온",
  ROUND(MIN(fw.min_temperature), 1) AS "최저 기온"
FROM
  fact.fact_weather fw
  JOIN dim.dim_time dt ON fw.time_key = dt.time_key
  JOIN dim.dim_area da ON fw.area_id = da.area_id
  JOIN week_range wr ON dt.date_value BETWEEN wr.start_of_last_week AND wr.end_of_last_week
WHERE
  1 = 1 [[
    AND da.area_name = {{area_name}}
]]
GROUP BY
  1,
  dt.date_value
ORDER BY
  dt.date_value;