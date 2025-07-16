import pendulum

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

import logging
import hashlib
import json
from io import BytesIO
import decimal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

# --- 설정 변수 ---
BUCKET_NAME = Variable.get("BUCKET_NAME")
S3_PREFIX = Variable.get("S3_PREFIX")
S3_PQ_PREFIX_COMM = Variable.get("S3_PQ_PREFIX_COMM")
S3_PQ_PREFIX_RSB = Variable.get("S3_PQ_PREFIX_RSB")
S3_PROCESSED_HISTORY_PREFIX = Variable.get("S3_PROCESSED_HISTORY_PREFIX")
REDSHIFT_IAM_ROLE = Variable.get("REDSHIFT_IAM_ROLE_ARN")
DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR")


@dag(
    dag_id="upload",
    schedule="30 9 * * *", 
    start_date=pendulum.datetime(2025, 7, 1, tz="Asia/Seoul"), 
    catchup=False,
    tags=["aws", "glue", "test"],
)
def commercial_data_backfill_night():
    run_glue_job = GlueJobOperator(
        task_id="commercial_data_backfill_night",
        job_name="de6-team1-glue-commercial-backfill-night", 
        replace_script_file=True
    )

    @task()
    def load_to_redshift(**context):
        """
        S3에서 Parquet 파일을 Redshift 테이블로 COPY INTO 명령을 사용하여 로드합니다.
        지정된 S3 경로 하위의 모든 Parquet 파일을 읽습니다.
        """
        redshift_hook = RedshiftSQLHook(redshift_conn_id="redshift_conn_id")
        ds_nodash = context["ds_nodash"]

        commercial_table_name = "source.source_commercial"
        commercial_parquet_path = f"s3://de6-team1-bucket/processed-data/commercial/{ds_nodash}/night/"
        rsb_parquet_path = f"s3://de6-team1-bucket/processed-data/commercial_rsb/{ds_nodash}/night/"

        copy_commercial_sql = f"""
            COPY {commercial_table_name} (
                source_id, area_code, area_name, congestion_level,
                total_payment_count, payment_amount_min, payment_amount_max,
                male_ratio, female_ratio, age_10s_ratio, age_20s_ratio,
                age_30s_ratio, age_40s_ratio, age_50s_ratio, age_60s_ratio,
                individual_consumer_ratio, corporate_consumer_ratio,
                observed_at, created_at
            )
            FROM '{commercial_parquet_path}'
            IAM_ROLE '{REDSHIFT_IAM_ROLE}'
            FORMAT AS PARQUET;
            """
        redshift_hook.run(copy_commercial_sql)

        rsb_table_name = "source.source_commercial_rsb"
        
        copy_rsb_sql = f"""
        COPY {rsb_table_name} (
            source_id, category_large, category_medium, category_congestion_level,
            category_payment_count, category_payment_min, category_payment_max,
            merchant_count, merchant_basis_month, observed_at, created_at
        )
        FROM '{rsb_parquet_path}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        FORMAT AS PARQUET;
        """
        redshift_hook.run(copy_rsb_sql)

    run_glue_job >> load_to_redshift()


commercial_data_backfill_night()    

