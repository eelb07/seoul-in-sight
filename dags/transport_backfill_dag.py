from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.models import Variable
import pendulum
import logging

log = logging.getLogger(__name__)

BUCKET_NAME = Variable.get("BUCKET_NAME")
REDSHIFT_IAM_ROLE_ARN = Variable.get("REDSHIFT_IAM_ROLE_ARN")
DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR")
TARGET_TABLE_BUS = "source.source_bus"
TARGET_TABLE_SUBWAY = "source.source_subway"
REDSHIFT_CONN_ID = Variable.get("REDSHIFT_CONN_ID")


kst = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="dag_transport_glue",
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2025, 7, 2, tz="Asia/Seoul"),
    catchup=False,
    tags=["12 hour", "glue"],
) as dag:
    run_glue_job = GlueJobOperator(
        task_id="transport_data_backfill_night",
        job_name="de6-team1-transport",
        script_location=f"s3://{BUCKET_NAME}/glue/jobs/transport_backfill_night.py",
        script_args={
            "JOB_NAME": "de6-team1-transport",
            "logical_date": "{{ data_interval_end }}",
        },
        region_name="ap-northeast-2",
        wait_for_completion=True,
        verbose=True,
        iam_role_name="de6-team1-glue-role",
        create_job_kwargs={
            "GlueVersion": "5.0",
            "NumberOfWorkers": 10,
            "WorkerType": "G.1X",
        },
    )

    @task
    def load_to_redshift(**context):
        process_time = context["data_interval_end"].in_timezone("Asia/Seoul")
        date_ymd = process_time.strftime("%Y%m%d")
        time_hm = process_time.strftime("%H%M")
        S3_BUS_PATH = f"s3://de6-team1-bucket/processed-data/transport/backfill_night/bus/{date_ymd}/{time_hm}/"
        S3_SUBWAY_PATH = f"s3://de6-team1-bucket/processed-data/transport/backfill_night/subway/{date_ymd}/{time_hm}/"

        log.info(f"Transferring parquet in {S3_BUS_PATH}")
        log.info(f"Transferring parquet in {S3_SUBWAY_PATH}")

        hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)

        sql_bus = f"""
            COPY {TARGET_TABLE_BUS}
            (
                area_code, area_name, total_geton_population_min, total_geton_population_max,
                total_getoff_population_min, total_getoff_population_max,
                geton_30min_population_min, geton_30min_population_max,
                getoff_30min_population_min, getoff_30min_population_max,
                geton_10min_population_min, geton_10min_population_max,
                getoff_10min_population_min, getoff_10min_population_max,
                geton_5min_population_min, geton_5min_population_max,
                getoff_5min_population_min, getoff_5min_population_max,
                station_count, station_count_basis_month,
                created_at, observed_at
            )
            FROM '{S3_BUS_PATH}'
            IAM_ROLE '{REDSHIFT_IAM_ROLE_ARN}'
            FORMAT AS PARQUET;
        """

        sql_subway = f"""
            COPY {TARGET_TABLE_SUBWAY}
            (
                area_code, area_name, total_geton_population_min, total_geton_population_max,
                total_getoff_population_min, total_getoff_population_max,
                geton_30min_population_min, geton_30min_population_max,
                getoff_30min_population_min, getoff_30min_population_max,
                geton_10min_population_min, geton_10min_population_max,
                getoff_10min_population_min, getoff_10min_population_max,
                geton_5min_population_min, geton_5min_population_max,
                getoff_5min_population_min, getoff_5min_population_max,
                station_count, station_count_basis_month,
                created_at, observed_at
            )
            FROM '{S3_SUBWAY_PATH}'
            IAM_ROLE '{REDSHIFT_IAM_ROLE_ARN}'
            FORMAT AS PARQUET;
        """

        hook.run(sql_bus)
        hook.run(sql_subway)
        log.info("✅ Redshift COPY 완료!")

    # run_dbt = BashOperator(
    #     task_id="run_dbt",
    #     bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --select fact_transport",
    # )

    glue_task = run_glue_job
    redshift_task = load_to_redshift()

    # glue_task >> redshift_task >> run_dbt
