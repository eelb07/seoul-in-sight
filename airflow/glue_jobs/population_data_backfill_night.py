import sys
import json
import boto3
import re

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col,
    to_timestamp,
    from_utc_timestamp,
    current_timestamp,
    year,
    month,
    dayofmonth,
    hour,
    date_format,
)
from pyspark.sql.types import StringType, IntegerType, FloatType, BooleanType

# Arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME", "logical_date"])
logical_time = datetime.fromisoformat(args["logical_date"]).replace(
    tzinfo=ZoneInfo("Asia/Seoul")
)
start_time = (logical_time - timedelta(days=1)).replace(hour=21, minute=0, second=0)
# start_time = logical_time.replace(hour=0, minute=0, second=0)
end_time = logical_time.replace(hour=9, minute=0, second=0)
# end_time = logical_time.replace(hour=23, minute=59, second=59)
now_str = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")

# S3 정보
bucket_name = "de6-team1-bucket"
raw_prefix = "raw-json"
processed_prefix = "processed-data/population/night-backfill"
history_key = "processed_history/population_glue.json"

# Spark 환경 초기화
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# 읽을 경로 생성
s3_paths = []
cursor = start_time
while cursor < end_time:
    s3_paths.append(
        f"s3://{bucket_name}/{raw_prefix}/{cursor.strftime('%Y%m%d')}/{cursor.strftime('%H')}*.json"
    )
    cursor += timedelta(hours=1)

print(s3_paths)
# spark로 json파일들을 병렬로 로드
df = spark.read.json(s3_paths)

# Spark DataFrame 스키마 정의
population_df = (
    df.filter(col("LIVE_PPLTN_STTS").isNotNull())
    .selectExpr("explode(LIVE_PPLTN_STTS) as data")
    .select(
        col("data.AREA_NM").cast(StringType()).alias("area_name"),
        col("data.AREA_CD").cast(StringType()).alias("area_code"),
        col("data.AREA_CONGEST_LVL").cast(StringType()).alias("congestion_label"),
        col("data.AREA_CONGEST_MSG").cast(StringType()).alias("congestion_message"),
        col("data.AREA_PPLTN_MIN").cast(IntegerType()).alias("population_min"),
        col("data.AREA_PPLTN_MAX").cast(IntegerType()).alias("population_max"),
        col("data.MALE_PPLTN_RATE").cast(FloatType()).alias("male_population_ratio"),
        col("data.FEMALE_PPLTN_RATE")
        .cast(FloatType())
        .alias("female_population_ratio"),
        col("data.PPLTN_RATE_0").cast(FloatType()).alias("age_0s_ratio"),
        col("data.PPLTN_RATE_10").cast(FloatType()).alias("age_10s_ratio"),
        col("data.PPLTN_RATE_20").cast(FloatType()).alias("age_20s_ratio"),
        col("data.PPLTN_RATE_30").cast(FloatType()).alias("age_30s_ratio"),
        col("data.PPLTN_RATE_40").cast(FloatType()).alias("age_40s_ratio"),
        col("data.PPLTN_RATE_50").cast(FloatType()).alias("age_50s_ratio"),
        col("data.PPLTN_RATE_60").cast(FloatType()).alias("age_60s_ratio"),
        col("data.PPLTN_RATE_70").cast(FloatType()).alias("age_70s_ratio"),
        col("data.RESNT_PPLTN_RATE").cast(FloatType()).alias("resident_ratio"),
        col("data.NON_RESNT_PPLTN_RATE").cast(FloatType()).alias("non_resident_ratio"),
        (col("data.REPLACE_YN") == "Y").cast(BooleanType()).alias("is_replaced"),
        to_timestamp(
            date_format(
                to_timestamp(col("data.PPLTN_TIME"), "yyyy-MM-dd HH:mm"),
                "yyyy-MM-dd HH:mm:ss",
            )
        ).alias("observed_at"),
        from_utc_timestamp(current_timestamp(), "Asia/Seoul").alias("created_at"),
    )
)

# S3에 저장된 Observed_at 처리 이력 불러오기
s3 = boto3.client("s3")
try:
    obj = s3.get_object(Bucket=bucket_name, Key=history_key)
    processed_dict = json.loads(obj["Body"].read().decode("utf-8"))
except s3.exceptions.NoSuchKey:
    processed_dict = {}

history_records = []
for area_code, items in processed_dict.items():
    for item in items:
        history_records.append(
            {"area_code": area_code, "observed_at": item["observed_at"]}
        )

"""
읽어온 json의 DataFrame과 S3에서 읽어온 Observed_at 처리이력의 DataFrame을 left_anti join을 통해
processed_df(처리이력 DataFrame)에 없는 row만 남김 -> 처리된 적없는 데이터
"""
if history_records:
    processed_df = spark.createDataFrame(history_records).withColumn(
        "observed_at", to_timestamp("observed_at", "yyyy-MM-dd HH:mm:ss")
    )
    filtered_df = population_df.join(
        processed_df, on=["area_code", "observed_at"], how="left_anti"
    )
else:
    filtered_df = population_df

# population_df 중복 제거
deduped_df = filtered_df.dropDuplicates(["area_code", "observed_at"])

# 파티션용 컬럼 추가
result_df = (
    deduped_df.withColumn("year", year("observed_at"))
    .withColumn("month", month("observed_at"))
    .withColumn("day", dayofmonth("observed_at"))
    .withColumn("hour", hour("observed_at"))
)

# Redshift에 적재하기 위해 컬럼 순서를 명시함
result_df.select(
    "area_name",
    "area_code",
    "congestion_label",
    "congestion_message",
    "population_min",
    "population_max",
    "male_population_ratio",
    "female_population_ratio",
    "age_0s_ratio",
    "age_10s_ratio",
    "age_20s_ratio",
    "age_30s_ratio",
    "age_40s_ratio",
    "age_50s_ratio",
    "age_60s_ratio",
    "age_70s_ratio",
    "resident_ratio",
    "non_resident_ratio",
    "is_replaced",
    "observed_at",
    "created_at",
    "year",
    "month",
    "day",
    "hour",
).write.mode("overwrite").partitionBy("year", "month", "day", "hour").parquet(
    f"s3://{bucket_name}/{processed_prefix}"
)

# Observed_at 처리 이력 업데이트
new_history = (
    result_df.select("area_code", "observed_at")
    .withColumn("observed_at", date_format("observed_at", "yyyy-MM-dd HH:mm:ss"))
    .collect()
)

for row in new_history:
    area_id = re.search(r"\d+$", row["area_code"]).group()
    ts = row["observed_at"]
    if str(area_id) not in processed_dict:
        processed_dict[str(area_id)] = []
    existing_ts = {item["observed_at"] for item in processed_dict.get(str(area_id), [])}
    if ts not in existing_ts:
        processed_dict[str(area_id)].append(
            {"observed_at": ts, "processed_at": now_str}
        )

# 처리 내역을 s3에 업로드
s3.put_object(
    Bucket=bucket_name,
    Key=history_key,
    Body=json.dumps(processed_dict, indent=2, ensure_ascii=False).encode("utf-8"),
    ContentType="application/json",
)

print(f"✅ 처리 완료 및 parquet 저장: {processed_prefix}")
job.commit()
