WITH numbers AS (
    SELECT (ROW_NUMBER() OVER ()) - 1 AS i
    FROM (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10) AS t1,
    (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10) AS t2,
    (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10) AS t3
),
extracted_polygon_ring AS (
    -- 1단계: geojson 문자열에서 'coordinates' 배열의 첫 번째 요소 (폴리곤 링) 추출
    SELECT
        area_code AS area_id,
        area_name,
        category,
        english_name,
        JSON_EXTRACT_ARRAY_ELEMENT_TEXT(JSON_EXTRACT_PATH_TEXT(geojson, 'coordinates'), 0) AS polygon_ring_coords_text
    FROM DIM.DIM_AREA_GEOJSON
),
flattened_coords AS (
    -- 2단계: 폴리곤 링 텍스트에서 각 [경도, 위도] 쌍을 개별 행으로 추출
    SELECT
        e.area_id,
        e.area_name,
        e.category,
        e.english_name,
        n.i AS coord_index,
        -- n.i는 추출할 좌표 쌍의 인덱스
        -- HERE IS THE CHANGE: Explicitly cast n.i to INTEGER
        JSON_EXTRACT_ARRAY_ELEMENT_TEXT(e.polygon_ring_coords_text, n.i::INTEGER) AS coord_pair_text -- 예: "[127.060275472604,37.5139033421982]"
    FROM
        extracted_polygon_ring e
    JOIN
        numbers n ON n.i < JSON_ARRAY_LENGTH(e.polygon_ring_coords_text)
    WHERE
        e.polygon_ring_coords_text IS NOT NULL
),
parsed_coords AS (
    -- 3단계: 각 [경도, 위도] 쌍 텍스트에서 경도와 위도를 분리하고 숫자로 변환
    SELECT
        area_id,
        area_name,
        category,
        english_name,
        coord_index,
        JSON_EXTRACT_ARRAY_ELEMENT_TEXT(coord_pair_text, 0::INTEGER)::DOUBLE PRECISION AS longitude,
        JSON_EXTRACT_ARRAY_ELEMENT_TEXT(coord_pair_text, 1::INTEGER)::DOUBLE PRECISION AS latitude
    FROM
        flattened_coords
    WHERE
        JSON_ARRAY_LENGTH(coord_pair_text) = 2 AND -- 정확히 두 개의 요소(경도, 위도)를 포함하는지 확인
        JSON_EXTRACT_ARRAY_ELEMENT_TEXT(coord_pair_text, 0::INTEGER) IS NOT NULL AND
        JSON_EXTRACT_ARRAY_ELEMENT_TEXT(coord_pair_text, 1::INTEGER) IS NOT NULL
)
-- 4단계: 그룹화하여 평균 위도와 경도 계산
SELECT
    area_id,
    area_name,
    category,
    english_name,
    ROUND(AVG(latitude), 8) AS avg_latitude,
    ROUND(AVG(longitude), 8) AS avg_longitude
FROM
    parsed_coords
GROUP BY
    area_id,
    area_name,
    category,
    english_name
ORDER BY
    area_id;
    