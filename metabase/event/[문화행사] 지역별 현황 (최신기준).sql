WITH
  unique_event_regions AS (
    SELECT DISTINCT
      fact.event_id,
      fact.area_id,
      SUBSTRING(fact.event_period, 1, 10) AS event_period_start_str,
      SUBSTRING(fact.event_period, 12, 10) AS event_period_end_str
    FROM
      fact.fact_event fact
      JOIN dim.dim_event dim ON fact.event_id = dim.event_id
  )
SELECT
  area.area_name AS "장소명",
  COUNT(*) AS "문화행사 수"
FROM
  unique_event_regions u
  JOIN dim.dim_area_geojson area ON u.area_id = area.area_id
WHERE
  CURRENT_DATE BETWEEN u.event_period_start_str::DATE AND u.event_period_end_str::DATE
GROUP BY
  area.area_name
ORDER BY
  "문화행사 수" DESC
LIMIT
  10;