import sys
import json
import hashlib
import boto3

from datetime import datetime, timedelta
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StringType,
    IntegerType,
    StructType,
    StructField,
    DecimalType,
)
from botocore.exceptions import ClientError
import pytz
from pyspark.sql import functions as F
from zoneinfo import ZoneInfo


# --------------------------
# 환경 설정
# --------------------------
BUCKET_NAME = "de6-team1-bucket"
S3_PREFIX = "raw-json"
S3_PROCESSED_HISTORY_PREFIX = "processed_history"
S3_PQ_PREFIX_COMM = "processed-data/commercial"
S3_PQ_PREFIX_RSB = "processed-data/commercial_rsb"
BASE_RAW_S3_PATH = f"s3://{BUCKET_NAME}/{S3_PREFIX}"
KST = ZoneInfo("Asia/Seoul")


# --------------------------
# 유틸 함수
# --------------------------
@F.udf(StringType())
def generate_source_id(area_code: str, observed_at: str) -> str:
    raw = f"{area_code}_{observed_at.replace(' ', '_')}"
    return hashlib.md5(raw.encode()).hexdigest()


def generate_s3_paths(start_dt: datetime, end_dt: datetime, base_s3_path: str) -> list:
    """
    주어진 시간 범위에 따라 S3 JSON 파일 경로 패턴 리스트를 생성합니다.
    예: ["s3://bucket/raw-json/20250714/21*.json", "s3://bucket/raw-json/20250715/08*.json"]
    """
    paths = []
    current = start_dt
    while current <= end_dt:  # end_dt까지 포함
        path_ymd = current.strftime("%Y%m%d")
        path_h = current.strftime("%H")
        paths.append(f"{base_s3_path}/{path_ymd}/{path_h}*.json")
        current += timedelta(hours=1)
    return paths


def parse_logical_date(logical_date_str):
    return datetime.fromisoformat(logical_date_str).astimezone(KST)


# --------------------------
# Glue 시작
# --------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "logical_date"])
logical_date = parse_logical_date(args["logical_date"])
spark = SparkSession.builder.appName("CommercialETLJob").getOrCreate()
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
s3 = boto3.client("s3")

# --------------------------
# 처리 이력 불러오기
# --------------------------
processed_observed_at_dict = {}
processed_observed_at_set = set()
processed_history_s3_key = f"{S3_PROCESSED_HISTORY_PREFIX}/commercial.json"
processed_history_broadcast = None

try:
    response = s3.get_object(Bucket=BUCKET_NAME, Key=processed_history_s3_key)
    processed_observed_at_dict = json.loads(response["Body"].read())

    processed_history_rows = []
    for area_id_str, values in processed_observed_at_dict.items():
        for value in values:
            processed_history_rows.append(
                Row(area_id=int(area_id_str), observed_at=value["observed_at"])
            )
            processed_observed_at_set.add((int(area_id_str), value["observed_at"]))

    processed_history_df = spark.createDataFrame(
        processed_history_rows, ["area_id", "observed_at"]
    )
    processed_history_broadcast = F.broadcast(processed_history_df)

    print(f"기존 처리 이력 로드 완료: {len(processed_observed_at_set)}개")
except ClientError as e:
    if e.response["Error"]["Code"] == "NoSuchKey":
        print("처리 이력 없음 - 첫 실행")
    else:
        raise

# --------------------------
# 처리 대상 시간 범위 설정
# --------------------------
kst = pytz.timezone("Asia/Seoul")
now_kst = logical_date
start_time = (logical_date - timedelta(days=1)).replace(
    hour=21, minute=0, second=0, microsecond=0
)
end_time = logical_date.replace(hour=8, minute=59, second=0)

print(f"조회 시간 범위: {start_time} ~ {end_time}")
# --------------------------
# S3에서 파일 처리
# --------------------------
s3_paths_to_read = []
current_time_iter = start_time
s3_paths_to_read = generate_s3_paths(start_time, end_time, BASE_RAW_S3_PATH)

if not s3_paths_to_read:
    print("처리할 새 파일이 없습니다.")
    commercial_df = spark.createDataFrame([], schema=StructType([]))
    commercial_rsb_df = spark.createDataFrame([], schema=StructType([]))
else:
    print(f"총 {len(s3_paths_to_read)}개의 파일을 Spark로 읽습니다.")
    raw_json_df = spark.read.json(s3_paths_to_read).withColumn(
        "input_file_path", F.input_file_name()
    )

    @F.udf(IntegerType())
    def extract_area_id_from_path(file_path: str) -> int:
        try:
            # 예: .../HHMM_12.json 에서 12 추출
            file_name = file_path.split("/")[-1]  # 파일 이름 (예: HHMM_12.json)
            # HHMM_ 부분과 .json 부분을 제거하고 숫자만 추출
            area_id_str = file_name.split("_")[1].split(".")[0]
            return int(area_id_str)
        except (IndexError, ValueError):
            return None

    base_df = (
        raw_json_df.select(
            F.col("AREA_CD").alias("area_code_raw"),
            F.col("AREA_NM").alias("area_name"),
            F.col("LIVE_CMRCL_STTS.CMRCL_TIME").alias("observed_at_raw"),
            F.lit(now_kst.strftime("%Y-%m-%d %H:%M:%S")).alias("created_at"),
            F.col("LIVE_CMRCL_STTS.*"),  # LIVE_CMRCL_STTS 하위의 모든 필드를 가져옴
            F.col("input_file_path"),  # 파일 경로 컬럼 유지
        )
        .withColumn(
            "observed_at",
            F.regexp_replace(
                F.col("observed_at_raw"),
                "(\\d{4})(\\d{2})(\\d{2}) (\\d{2})(\\d{2})",
                "$1-$2-$3 $4:$5:00",
            ),
        )
        .withColumn(
            "source_id",
            generate_source_id(F.col("area_code_raw"), F.col("observed_at_raw")),
        )
        .withColumn(
            "file_area_id",  # 파일 경로에서 추출한 area_id
            extract_area_id_from_path(F.col("input_file_path")),
        )
        .drop("input_file_path")
    )  # 임시 컬럼 삭제

    # --------------------------
    # Spark DataFrame용 Schema 정의
    # --------------------------

    # Commercial DataFrame Schema
    commercial_schema = StructType(
        [
            StructField("source_id", StringType(), True),
            StructField("area_code", StringType(), True),
            StructField("area_name", StringType(), True),
            StructField("congestion_level", StringType(), True),
            StructField("total_payment_count", IntegerType(), True),
            StructField("payment_amount_min", IntegerType(), True),
            StructField("payment_amount_max", IntegerType(), True),
            StructField("male_ratio", DecimalType(5, 2), True),
            StructField("female_ratio", DecimalType(5, 2), True),
            StructField("age_10s_ratio", DecimalType(5, 2), True),
            StructField("age_20s_ratio", DecimalType(5, 2), True),
            StructField("age_30s_ratio", DecimalType(5, 2), True),
            StructField("age_40s_ratio", DecimalType(5, 2), True),
            StructField("age_50s_ratio", DecimalType(5, 2), True),
            StructField("age_60s_ratio", DecimalType(5, 2), True),
            StructField("individual_consumer_ratio", DecimalType(5, 2), True),
            StructField("corporate_consumer_ratio", DecimalType(5, 2), True),
            StructField("observed_at", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("area_id_for_join", IntegerType(), True),
        ]
    )

    # 상권 일반 데이터 추출 및 타입 변환
    commercial_df = base_df.select(
        F.col("source_id"),
        F.col("area_code_raw").alias("area_code"),
        F.col("area_name"),
        F.col("AREA_CMRCL_LVL").alias("congestion_level"),
        F.col("AREA_SH_PAYMENT_CNT").cast(IntegerType()).alias("total_payment_count"),
        F.col("AREA_SH_PAYMENT_AMT_MIN")
        .cast(IntegerType())
        .alias("payment_amount_min"),
        F.col("AREA_SH_PAYMENT_AMT_MAX")
        .cast(IntegerType())
        .alias("payment_amount_max"),
        F.col("CMRCL_MALE_RATE").cast(DecimalType(5, 2)).alias("male_ratio"),
        F.col("CMRCL_FEMALE_RATE").cast(DecimalType(5, 2)).alias("female_ratio"),
        F.col("CMRCL_10_RATE").cast(DecimalType(5, 2)).alias("age_10s_ratio"),
        F.col("CMRCL_20_RATE").cast(DecimalType(5, 2)).alias("age_20s_ratio"),
        F.col("CMRCL_30_RATE").cast(DecimalType(5, 2)).alias("age_30s_ratio"),
        F.col("CMRCL_40_RATE").cast(DecimalType(5, 2)).alias("age_40s_ratio"),
        F.col("CMRCL_50_RATE").cast(DecimalType(5, 2)).alias("age_50s_ratio"),
        F.col("CMRCL_60_RATE").cast(DecimalType(5, 2)).alias("age_60s_ratio"),
        F.col("CMRCL_PERSONAL_RATE")
        .cast(DecimalType(5, 2))
        .alias("individual_consumer_ratio"),
        F.col("CMRCL_CORPORATION_RATE")
        .cast(DecimalType(5, 2))
        .alias("corporate_consumer_ratio"),
        F.col("observed_at"),
        F.col("created_at"),
        F.col("file_area_id").alias("area_id_for_join"),  # 조인용 area_id
    ).distinct()

    # 명시적 스키마 적용 및 타임스탬프 변환
    commercial_df = spark.createDataFrame(commercial_df.rdd, schema=commercial_schema)
    commercial_df = commercial_df.withColumn(
        "observed_at", F.to_timestamp(F.col("observed_at"))
    ).withColumn("created_at", F.to_timestamp(F.col("created_at")))

    commercial_df.drop("source_id").show(truncate=False)

    # Commercial RSB DataFrame Schema
    commercial_rsb_schema = StructType(
        [
            StructField("source_id", StringType(), True),
            StructField("category_large", StringType(), True),
            StructField("category_medium", StringType(), True),
            StructField("category_congestion_level", StringType(), True),
            StructField("category_payment_count", IntegerType(), True),
            StructField("category_payment_min", IntegerType(), True),
            StructField("category_payment_max", IntegerType(), True),
            StructField("merchant_count", IntegerType(), True),
            StructField("merchant_basis_month", StringType(), True),
            StructField("observed_at", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("area_id_for_join", IntegerType(), True),
        ]
    )

    # 상권 RSB 데이터 추출 및 타입 변환
    commercial_rsb_df = (
        base_df.withColumn(
            "rsb_data",
            F.explode_outer("CMRCL_RSB"),  # CMRCL_RSB 배열을 explode
        )
        .select(
            F.col("source_id"),
            F.col("rsb_data.RSB_LRG_CTGR").alias("category_large"),
            F.col("rsb_data.RSB_MID_CTGR").alias("category_medium"),
            F.col("rsb_data.RSB_PAYMENT_LVL").alias("category_congestion_level"),
            F.col("rsb_data.RSB_SH_PAYMENT_CNT")
            .cast(IntegerType())
            .alias("category_payment_count"),
            F.col("rsb_data.RSB_SH_PAYMENT_AMT_MIN")
            .cast(IntegerType())
            .alias("category_payment_min"),
            F.col("rsb_data.RSB_SH_PAYMENT_AMT_MAX")
            .cast(IntegerType())
            .alias("category_payment_max"),
            F.col("rsb_data.RSB_MCT_CNT").cast(IntegerType()).alias("merchant_count"),
            F.col("rsb_data.RSB_MCT_TIME").alias("merchant_basis_month"),
            F.col("observed_at"),
            F.col("created_at"),
            F.col("file_area_id").alias("area_id_for_join"),  # 조인용 area_id
        )
        .distinct()
    )

    # 명시적 스키마 적용 및 타임스탬프 변환
    commercial_rsb_df = spark.createDataFrame(
        commercial_rsb_df.rdd, schema=commercial_rsb_schema
    )
    commercial_rsb_df = commercial_rsb_df.withColumn(
        "observed_at", F.to_timestamp(F.col("observed_at"))
    ).withColumn("created_at", F.to_timestamp(F.col("created_at")))

    commercial_rsb_df.drop("source_id").show(truncate=False)

    # --------------------------
    # 처리 이력에 따라 이미 처리된 데이터 필터링
    # --------------------------
    # processed_history_broadcast가 정의된 경우에만 필터링 로직 실행
    if processed_history_broadcast is not None:
        commercial_df = commercial_df.join(
            processed_history_broadcast,
            (
                commercial_df["area_id_for_join"]
                == processed_history_broadcast["area_id"]
            )
            & (
                commercial_df["observed_at"]
                == processed_history_broadcast["observed_at"]
            ),
            "left_anti",
        )

        commercial_rsb_df = commercial_rsb_df.join(
            processed_history_broadcast,
            (
                commercial_rsb_df["area_id_for_join"]
                == processed_history_broadcast["area_id"]
            )
            & (
                commercial_rsb_df["observed_at"]
                == processed_history_broadcast["observed_at"]
            ),
            "left_anti",
        )

    newly_processed_comm_data = commercial_df.select(
        F.col("area_id_for_join").alias("area_id"), "observed_at"
    ).collect()
    newly_processed_rsb_data = commercial_rsb_df.select(
        F.col("area_id_for_join").alias("area_id"), "observed_at"
    ).collect()
    commercial_df = commercial_df.drop("area_id_for_join")
    commercial_rsb_df = commercial_rsb_df.drop("area_id_for_join")

    # 필터링 후에도 데이터가 남아있는지 확인
    if commercial_df.count() == 0 and commercial_rsb_df.count() == 0:
        print("기존 처리 이력에 따라 처리할 새로운 데이터가 없습니다.")


def delete_s3_prefix(bucket, prefix):
    print(f"S3 {bucket}/{prefix} 디렉터리 내 객체 삭제 시작...")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    delete_keys = {"Objects": []}
    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                delete_keys["Objects"].append({"Key": obj["Key"]})

            if len(delete_keys["Objects"]) >= 1000:
                s3.delete_objects(Bucket=bucket, Delete=delete_keys)
                delete_keys = {"Objects": []}

    if len(delete_keys["Objects"]) > 0:
        s3.delete_objects(Bucket=bucket, Delete=delete_keys)
    print(f"S3 {bucket}/{prefix} 디렉터리 내 객체 삭제 완료.")


output_comm_path_prefix = f"{S3_PQ_PREFIX_COMM}/{now_kst.strftime('%Y%m%d')}/night"
output_rsb_path_prefix = f"{S3_PQ_PREFIX_RSB}/{now_kst.strftime('%Y%m%d')}/night"

delete_s3_prefix(BUCKET_NAME, output_comm_path_prefix)
delete_s3_prefix(BUCKET_NAME, output_rsb_path_prefix)


# --------------------------
# 저장 및 업로드
# --------------------------
if commercial_df.count() > 0:
    output_path = f"s3://{BUCKET_NAME}/{output_comm_path_prefix}"
    commercial_df.write.mode("append").parquet(output_path)
    print(f"상권 일반 데이터 저장 완료: {output_path}")

if commercial_rsb_df.count() > 0:
    output_path_rsb = f"s3://{BUCKET_NAME}/{output_rsb_path_prefix}"
    commercial_rsb_df.write.mode("append").parquet(output_path_rsb)
    print(f"상권 RSB 데이터 저장 완료: {output_path_rsb}")

# --------------------------
# 처리 이력 S3 업로드
# --------------------------
for row in newly_processed_comm_data + newly_processed_rsb_data:
    area_id_str = str(row.area_id)
    if area_id_str not in processed_observed_at_dict:
        processed_observed_at_dict[area_id_str] = []

    observed_at_str = (
        row.observed_at.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(row.observed_at, datetime)
        else row.observed_at
    )
    if not any(
        d["observed_at"] == observed_at_str
        for d in processed_observed_at_dict[area_id_str]
    ):
        processed_observed_at_dict[area_id_str].append(
            {
                "observed_at": observed_at_str,
                "processed_at": now_kst.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

try:
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=processed_history_s3_key,
        Body=json.dumps(processed_observed_at_dict, indent=4),
    )
    print(f"처리 이력 업데이트 완료: s3://{BUCKET_NAME}/{processed_history_s3_key}")
except ClientError as e:
    print(f"처리 이력 저장 실패: {e}")
