from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.bash import BashOperator
from datetime import timedelta

import pendulum
import json
import logging
import textwrap

logger = logging.getLogger(__name__)
S3_BUCKET = "de6-team1-bucket"
DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR")


@dag(
    dag_id="population_data_backfill_night",
    start_date=pendulum.datetime(2025, 7, 1, tz="Asia/Seoul"),
    schedule="0 9 * * *",
    doc_md=textwrap.dedent("""
        #인구데이터 Backfill용 DAG
        EC2 서버가 꺼져있는 21시부터 다음날 9시까지의 처리안된 데이터를 Backfill함

        - **run_glue_job**: S3에 저장된 glue 스크립트를 불러서 12시간 데이터를 parquet으로 변환 후 s3에 저장
        - **load_to_redshift**: 처리된 parquet을 Manifest 파일을 사용하여 redshift source 테이블에 적재
        - **run_dbt**: fact_population 모델을 실행하여 fact 테이블에 적재
    """),
    catchup=False,
    tags=["glue", "population"],
)
def population_data_backfill_night():
    run_glue_job = GlueJobOperator(
        task_id="run_glue_backfill_night",
        aws_conn_id="aws_conn_id",
        job_name="de6-team1-glue-population",
        region_name="ap-northeast-2",
        iam_role_name="de6-team1-glue-role",
        s3_bucket=S3_BUCKET,
        script_location="s3://de6-team1-bucket/glue/jobs/population_data_backfill_night.py",
        script_args={
            "--JOB_NAME": "de6-team1-glue-population",
            "--logical_date": "{{ logical_date.in_timezone('Asia/Seoul').strftime('%Y-%m-%dT%H:%M:%S') }}",
        },
        create_job_kwargs={
            "GlueVersion": "5.0",
            "WorkerType": "G.1X",
            "NumberOfWorkers": 10,
        },
    )

    @task
    def load_to_redshift(**context):
        """
        S3의 Parquet 파일 목록으로 구성된 Manifest 파일 생성 후, Redshift의 COPY 명령어 사용해 해당 파일들을 source_population 테이블에 적재함
        """
        utc_time = context["logical_date"]
        kst_time = utc_time.in_timezone("Asia/Seoul")
        start_time = kst_time.subtract(days=1).replace(hour=21, minute=0, second=0)
        end_time = kst_time.replace(hour=9, minute=0, second=0)

        s3_hook = S3Hook("aws_conn_id")
        manifest_entries = {"entries": []}

        cursor = start_time
        while cursor < end_time:
            prefix = (
                f"processed-data/population/night-backfill/"
                f"year={cursor.year}/"
                f"month={cursor.month}/"
                f"day={cursor.day}/"
                f"hour={cursor.hour}/"
            )
            parquet_keys = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=prefix)
            for key in parquet_keys:
                if key.endswith(".parquet"):
                    obj = s3_hook.get_key(key, S3_BUCKET)
                    manifest_entries["entries"].append(
                        {
                            "url": f"s3://{S3_BUCKET}/{key}",
                            "mandatory": True,
                            "meta": {"content_length": obj.content_length},
                        }
                    )

            cursor += timedelta(hours=1)

        manifest_filename = f"population_night_backfill_data_load_{kst_time.strftime('%Y%m%d%H%M')}.json"
        manifest_key = f"redshift-manifests/population/{manifest_filename}"
        manifest_content = json.dumps(manifest_entries, indent=2)

        s3_hook.load_string(
            string_data=manifest_content,
            key=manifest_key,
            bucket_name=S3_BUCKET,
            replace=True,
        )

        manifest_s3_path = f"s3://{S3_BUCKET}/{manifest_key}"

        redshift = RedshiftSQLHook("redshift_dev_db")
        source_table = "source.source_population"
        redshift_iam_role_arn = Variable.get("REDSHIFT_IAM_ROLE_ARN")

        redshift.run("BEGIN;")

        redshift.run(f"""
            COPY {source_table}
            FROM '{manifest_s3_path}'
            IAM_ROLE '{redshift_iam_role_arn}'
            FORMAT AS PARQUET
            MANIFEST;
        """)
        logger.info(f"Copying data to Redshift using manifest from: {manifest_s3_path}")
        redshift.run("COMMIT;")

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps && dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --select fact_population",
    )

    load = load_to_redshift()
    run_glue_job >> load >> run_dbt


dag = population_data_backfill_night()
