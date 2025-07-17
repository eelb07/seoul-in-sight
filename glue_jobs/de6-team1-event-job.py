import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "input_path",
        "output_path",
        "observed_date",
    ],
)
# 파라미터 정의
JOB_NAME = args["JOB_NAME"]
INPUT_PATH = args["input_path"]
OUTPUT_PATH = args["output_path"]
OBSERVED_DATE = args["observed_date"]

# Glue/Spark 컨텍스트 초기화
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

# JSON 읽기
# 콤마로 묶인 여러 S3 URI를 리스트로 분리해 전달
paths = INPUT_PATH.split(",")
df = spark.read.json(paths)

# EVENT_STTS 배열 평면화
flat = df.filter(F.col("EVENT_STTS").isNotNull() & (F.size("EVENT_STTS") > 0)).select(
    F.col("AREA_CD").alias("area_code"),
    F.col("AREA_NM").alias("area_name"),
    F.explode("EVENT_STTS").alias("e"),
)

# 필요한 컬럼만 선택 & 이름 변경
selected = flat.select(
    "area_code",
    "area_name",
    F.col("e.EVENT_NM").alias("event_name"),
    F.col("e.EVENT_PERIOD").alias("event_period"),
    F.col("e.EVENT_PLACE").alias("event_place"),
    F.col("e.EVENT_X").alias("event_x"),
    F.col("e.EVENT_Y").alias("event_y"),
    # PAY_YN 필드가 "Y"/"N"/null 이면 Boolean으로 캐스팅
    F.when(F.col("e.PAY_YN") == "Y", True)
    .when(F.col("e.PAY_YN") == "N", False)
    .otherwise(None)
    .alias("is_paid"),
    F.col("e.THUMBNAIL").alias("thumbnail_url"),
    F.col("e.URL").alias("event_url"),
    F.col("e.EVENT_ETC_DETAIL").alias("event_extra_detail"),
)

# 6) observed_at 과 created_at 추가
with_dates = selected.withColumn(
    "observed_at",
    # 전달받은 YYYY-MM-DD 문자열을 타임스탬프로 변환
    F.to_timestamp(F.lit(OBSERVED_DATE), "yyyy-MM-dd"),
).withColumn("created_at", F.current_timestamp())

# Parquet으로 저장 (overwrite)
with_dates.write.mode("overwrite").parquet(OUTPUT_PATH)

# Job 완료
job.commit()
