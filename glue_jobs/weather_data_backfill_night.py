import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import (
    col,
    explode,
    to_timestamp,
    current_timestamp,
    from_utc_timestamp,
    year,
    month,
    dayofmonth,
    hour,
)

# -- Constants
KST = ZoneInfo("Asia/Seoul")
BUCKET_NAME = "de6-team1-bucket"
BASE_RAW_S3_PATH = f"s3://{BUCKET_NAME}/raw-json"
OUTPUT_PATH = f"s3://{BUCKET_NAME}/processed-data/weather/night-backfill/"


# -- Functions
def parse_logical_date(logical_date_str):
    return datetime.fromisoformat(logical_date_str).astimezone(KST)


def generate_s3_paths(start_dt, end_dt):
    # Generate S3 paths from the past 12 hours
    current = start_dt
    paths = []
    while current < end_dt:
        path_ymd = current.strftime("%Y%m%d")
        path_h = current.strftime("%H")
        paths.append(f"{BASE_RAW_S3_PATH}/{path_ymd}/{path_h}*.json")
        current += timedelta(hours=1)
    return paths


def flatten_and_transform(df):
    # Flatten nested WEATHER_STTS array
    exploded_df = df.filter(
        F.col("WEATHER_STTS").isNotNull() & (F.size("WEATHER_STTS") > 0)
    ).select(
        col("AREA_CD").alias("area_code"),
        col("AREA_NM").alias("area_name"),
        explode(col("WEATHER_STTS")).alias("weather_data"),
    )

    # Select and cast required columns
    final_df = exploded_df.select(
        col("area_code"),
        col("area_name"),
        col("weather_data.TEMP").cast(FloatType()).alias("temperature"),
        col("weather_data.SENSIBLE_TEMP")
        .cast(FloatType())
        .alias("sensible_temperature"),
        col("weather_data.MAX_TEMP").cast(FloatType()).alias("max_temperature"),
        col("weather_data.MIN_TEMP").cast(FloatType()).alias("min_temperature"),
        col("weather_data.HUMIDITY").cast(IntegerType()).alias("humidity"),
        col("weather_data.WIND_DIRCT").alias("wind_direction"),
        col("weather_data.WIND_SPD").cast(FloatType()).alias("wind_speed"),
        col("weather_data.PRECIPITATION").alias("precipitation"),
        col("weather_data.PRECPT_TYPE").alias("precipitation_type"),
        col("weather_data.PCP_MSG").alias("precipitation_message"),
        col("weather_data.SUNRISE").alias("sunrise"),
        col("weather_data.SUNSET").alias("sunset"),
        col("weather_data.UV_INDEX_LVL").cast(IntegerType()).alias("uv_index_level"),
        col("weather_data.UV_INDEX").alias("uv_index_desc"),
        col("weather_data.UV_MSG").alias("uv_message"),
        col("weather_data.PM25_INDEX").alias("pm25_index"),
        col("weather_data.PM25").cast(IntegerType()).alias("pm25_value"),
        col("weather_data.PM10_INDEX").alias("pm10_index"),
        col("weather_data.PM10").cast(IntegerType()).alias("pm10_value"),
        col("weather_data.AIR_IDX").alias("air_quality_index"),
        F.when(col("weather_data.AIR_IDX_MVL") == "점검중", None)
        .otherwise(col("weather_data.AIR_IDX_MVL").cast(FloatType()))
        .alias("air_quality_value"),
        col("weather_data.AIR_IDX_MAIN").alias("air_quality_main"),
        col("weather_data.AIR_MSG").alias("air_quality_message"),
        to_timestamp(col("weather_data.WEATHER_TIME"), "yyyy-MM-dd HH:mm").alias(
            "observed_at"
        ),
        from_utc_timestamp(current_timestamp(), "Asia/Seoul").alias("created_at"),
    )

    # Deduplicate by keeping max values per area and timestamp
    grouping_columns = ["area_code", "observed_at"]
    agg_exprs = [
        F.max(col(c)).alias(c) for c in final_df.columns if c not in grouping_columns
    ]
    grouped_df = final_df.groupBy(grouping_columns).agg(*agg_exprs)

    return (
        grouped_df.withColumn("year", year(col("observed_at")))
        .withColumn("month", month(col("observed_at")))
        .withColumn("day", dayofmonth(col("observed_at")))
        .withColumn("hour", hour(col("observed_at")))
    )


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "logical_date"])
    logical_date = parse_logical_date(args["logical_date"])
    start_dt = logical_date - timedelta(hours=12)
    end_dt = logical_date
    paths = generate_s3_paths(start_dt, end_dt)

    # Initialize Spark and Glue contexts
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    df = spark.read.json(paths)
    transformed_df = flatten_and_transform(df)

    # Reorder columns to match Redshift COPY and write to S3 in parquet format
    transformed_df.select(
        "area_code",
        "observed_at",
        "area_name",
        "temperature",
        "sensible_temperature",
        "max_temperature",
        "min_temperature",
        "humidity",
        "wind_direction",
        "wind_speed",
        "precipitation",
        "precipitation_type",
        "precipitation_message",
        "sunrise",
        "sunset",
        "uv_index_level",
        "uv_index_desc",
        "uv_message",
        "pm25_index",
        "pm25_value",
        "pm10_index",
        "pm10_value",
        "air_quality_index",
        "air_quality_value",
        "air_quality_main",
        "air_quality_message",
        "created_at",
        "year",
        "month",
        "day",
        "hour",
    ).write.mode("overwrite").partitionBy("year", "month", "day", "hour").parquet(
        OUTPUT_PATH
    )

    job.commit()


if __name__ == "__main__":
    main()
