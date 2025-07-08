from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator

# === 상수 설정 ===
S3_PATH_TEMPLATE: str = (
    "s3://de6-team1-bucket/processed-data/event/{{ ds_nodash }}.parquet"
)
IAM_ROLE_ARN: str = "arn:aws:iam::862327261051:role/service-role/AmazonRedshift-CommandsAccessRole-20250703T095914"
REDSHIFT_CONN_ID: str = "redshift_conn_id"
TARGET_TABLE: str = "source.source_event"


@dag(
    dag_id="load_source_event",
    description="Silver - S3의 Parquet을 RedShift에 적재",
    schedule="10 10 * * *",  # 매일 오전 10시 10분
    start_date=pendulum.datetime(2025, 7, 2, tz="Asia/Seoul"),
    catchup=True,
    tags=["silver", "redshift"],
    default_args={"retries": 1},
)
def load_source_event_dag():
    PostgresOperator(
        task_id="copy_parquet_to_redshift",
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=f"""
            COPY {TARGET_TABLE}
            FROM '{S3_PATH_TEMPLATE}'
            IAM_ROLE '{IAM_ROLE_ARN}'
            FORMAT AS PARQUET;
        """,
    )


load_source_event_dag()
