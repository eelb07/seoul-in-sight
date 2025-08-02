import json
import logging
from airflow.models.variable import Variable
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import timedelta
import pendulum


logger = logging.getLogger(__name__)
BUCKET_NAME = Variable.get("BUCKET_NAME")
DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR")

default_args = {
    "onwer": "dongyeong",
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
}


@dag(
    dag_id="weather_data_backfill_night",
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2025, 7, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["weather", "glue", "backfill"],
    default_args=default_args,
    doc_md="""
    # 기상데이터 Backfill용 DAG
    이 DAG는 매일 오전 9시에 실행되어, 전날 밤 9시부터 오전 9시까지의 12시간 동안 누락되었을 수 있는 기상 데이터를 백필합니다.

    ### 주요 작업
    1. **Glue Job**: S3의 원시 JSON 데이터를 Parquet으로 변환하여 저장합니다.
    2. **Redshift COPY**: Manifest 파일을 사용하여 변환된 데이터를 Redshift의 source.source_weather 테이블에 적재합니다.
    3. **dbt run**: fact_weather 모델을 실행하여 팩트 테이블을 증분적재 합니다.

    ### 스케줄
    - 매일 오전 9시 (KST)
    - 전날 밤 9시 ~ 당일 오전 9시 데이터 처리
    """,
)
def weather_data_backfill_night():
    run_glue_job = GlueJobOperator(
        task_id="run_glue_job",
        aws_conn_id="aws_default",
        region_name="ap-northeast-2",
        iam_role_name="de6-team1-glue-role",
        job_name="de6-team1-glue-weather-backfill-night",
        s3_bucket=BUCKET_NAME,
        script_location=f"s3://{BUCKET_NAME}/glue/jobs/weather_backfill_night_etl.py",
        script_args={
            "--JOB_NAME": "weather_data_backfill_night",
            "--logical_date": "{{ logical_date }}",
        },
        create_job_kwargs={
            "GlueVersion": "5.0",
            "WorkerType": "G.1X",
            "NumberOfWorkers": 10,
        },
    )

    @task
    def load_to_redshift(**context):
        utc_time = context["logical_date"]
        kst_time = utc_time.in_timezone("Asia/Seoul")
        start_dt = kst_time - timedelta(hours=12)
        end_dt = kst_time

        s3_hook = S3Hook("aws_default")
        manifest_entries = {"entries": []}

        # Build manifest entries from parquet files by hourly partitions
        current_dt = start_dt
        while current_dt < end_dt:
            prefix = (
                f"processed-data/weather/night-backfill/"
                f"year={current_dt.year}/"
                f"month={current_dt.month}/"
                f"day={current_dt.day}/"
                f"hour={current_dt.hour}/"
            )
            parquet_keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=prefix)
            for key in parquet_keys:
                if key.endswith(".parquet"):
                    obj = s3_hook.get_key(key, BUCKET_NAME)
                    manifest_entries["entries"].append(
                        {
                            "url": f"s3://{BUCKET_NAME}/{key}",
                            "mandatory": True,
                            "meta": {"content_length": obj.content_length},
                        }
                    )

            current_dt += timedelta(hours=1)

        if not manifest_entries["entries"]:
            logger.warning("No parquet files found to create manifest.")
            return

        # Upload manifest to S3
        manifest_filename = (
            f"weather_night_backfill_data_load_{kst_time.strftime('%Y%m%d%H%M')}.json"
        )
        manifest_key = f"redshift-manifests/weather/{manifest_filename}"
        manifest_content = json.dumps(manifest_entries, indent=2)

        s3_hook.load_string(
            string_data=manifest_content,
            key=manifest_key,
            bucket_name=BUCKET_NAME,
            replace=True,
        )

        manifest_s3_path = f"s3://{BUCKET_NAME}/{manifest_key}"
        logger.info(f"Manifest file uploaded to: {manifest_s3_path}")

        # Execute Redshift COPY command using manifest
        redshift = RedshiftSQLHook("redshift_conn_id")
        target_table = "source.source_weather"
        redshift_iam_role_arn = Variable.get("REDSHIFT_IAM_ROLE_ARN")

        delete_start_dt = start_dt.format("YYYY-MM-DD HH:00:00")
        delete_end_dt = end_dt.format("YYYY-MM-DD HH:00:00")

        redshift.run("BEGIN;")
        redshift.run(f"""
            DELETE FROM {target_table}
            WHERE observed_at BETWEEN '{delete_start_dt}' AND '{delete_end_dt}';
        """)
        logger.info(
            f"Deleting existing data for range: {delete_start_dt} to {delete_end_dt}"
        )

        redshift.run(f"""
            COPY {target_table} (
                area_code, observed_at, area_name, temperature,
                sensible_temperature, max_temperature, min_temperature,
                humidity, wind_direction, wind_speed, precipitation,
                precipitation_type, precipitation_message, sunrise, sunset,
                uv_index_level, uv_index_desc, uv_message,
                pm25_index, pm25_value, pm10_index, pm10_value,
                air_quality_index, air_quality_value, air_quality_main, air_quality_message,
                created_at
            )
            FROM '{manifest_s3_path}'
            IAM_ROLE '{redshift_iam_role_arn}'
            FORMAT AS PARQUET
            MANIFEST;
        """)
        logger.info(f"Copying data to Redshift using manifest from: {manifest_s3_path}")
        redshift.run("COMMIT;")

    @task.bash
    def run_dbt():
        return f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --select fact_weather"

    load = load_to_redshift()
    dbt = run_dbt()
    run_glue_job >> load >> dbt


weather_data_backfill_night()
