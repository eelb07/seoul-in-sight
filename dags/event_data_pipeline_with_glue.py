import json
import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.bash import BashOperator

# --- 설정 변수 ---
BUCKET_NAME: str = Variable.get("BUCKET_NAME")
S3_PREFIX = Variable.get("S3_PREFIX")
S3_PQ_PREFIX_EVENT = Variable.get("S3_PQ_PREFIX_EVENT")
REDSHIFT_IAM_ROLE_ARN: str = Variable.get("REDSHIFT_IAM_ROLE_ARN")
DBT_PROJECT_DIR: str = Variable.get("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR")

log = logging.getLogger(__name__)


@dag(
    dag_id="event_data_pipeline_with_glue",
    schedule="0 11 * * *",
    start_date=pendulum.datetime(2025, 7, 2, tz="Asia/Seoul"),
    catchup=True,
    tags=["event", "glue"],
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10)},
    doc_md="""
    # 이벤트 데이터 파이프라인
    1) GlueJobOperator로 S3 raw-json에서 EVENT_STTS만 추출
    2) Parquet으로 변환해 S3에 저장
    3) downstream Redshift 적재 & dbt 실행(기존 Task)
    """,
)
def event_data_pipeline_with_glue():
    """처리할 파일 하나씩 골라서 넘기는 Task"""

    @task
    def pick_event_file(ds_nodash: str) -> str:
        hook = S3Hook("aws_default")
        # hhmm '003'로 시작하는 파일만 리스트업
        prefix = f"{S3_PREFIX}/{ds_nodash}/003"
        keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix=prefix) or []
        if not keys:
            raise ValueError(f"{ds_nodash}에 처리할 이벤트 파일이 없습니다.")

        # location 별 첫 번째 파일만 선택
        loc_map = {}
        for key in sorted(keys):
            filename = key.split("/")[-1]
            loc = filename.split("_")[1].split(".")[0]
            # location별로 loc_map에 없으면 첫번째(가장 빠른) 파일 경로를 저장
            loc_map.setdefault(loc, key)

        # S3 URI 형태로 변환 후 쉼표로 묶어서 반환
        selected_uris = [f"s3://{BUCKET_NAME}/{path}" for path in loc_map.values()]
        return ",".join(selected_uris)

    # Glue로 JSON → Parquet 변환
    run_event_glue = GlueJobOperator(
        task_id="run_event_glue",
        aws_conn_id="aws_default",
        region_name="ap-northeast-2",
        iam_role_name="de6-team1-glue-role",
        job_name="de6-team1-event-job",
        script_location=f"s3://{BUCKET_NAME}/glue/jobs/de6-team1-event-job.py",
        replace_script_file=True,  # s3 script 사용
        script_args={
            "--JOB_NAME": "de6-team1-event-job",
            "--input_path": "{{ ti.xcom_pull(task_ids='pick_event_file') }}",
            "--output_path": f"s3://{BUCKET_NAME}/{S3_PQ_PREFIX_EVENT}/{{{{ ds_nodash }}}}/",
            "--observed_date": "{{ ds }}",  # YYYY-MM-DD
        },
        create_job_kwargs={
            "GlueVersion": "5.0",
            "WorkerType": "G.1X",
            "NumberOfWorkers": 10,
        },
    )

    # Parquet → Redshift 로딩
    @task
    def load_to_redshift(**context):
        ds: str = context["ds"]  # 'YYYY-MM-DD'
        ds_nodash: str = context["ds_nodash"]  # 'YYYYMMDD'

        s3_hook = S3Hook("aws_default")
        manifest = {"entries": []}
        prefix = f"{S3_PQ_PREFIX_EVENT}/{ds_nodash}/"
        for key in s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=prefix) or []:
            if key.endswith(".parquet"):
                obj = s3_hook.get_key(key, BUCKET_NAME)
                manifest["entries"].append(
                    {
                        "url": f"s3://{BUCKET_NAME}/{key}",
                        "mandatory": True,  # 필수 파일 여부
                        "meta": {"content_length": obj.content_length},
                    }
                )

        if not manifest["entries"]:
            log.warning(
                "parquet 파일을 찾을 수 없어 %s에 대한 Redshift 적재를 건너뜁니다.", ds
            )
            return

        # manifest 업로드
        manifest_key = f"redshift-manifests/event/event_manifest_{ds_nodash}.json"
        s3_hook.load_string(
            string_data=json.dumps(manifest, indent=2),
            key=manifest_key,
            bucket_name=BUCKET_NAME,
            replace=True,
        )
        manifest_s3_path = f"s3://{BUCKET_NAME}/{manifest_key}"
        log.info("manifest를 '%s'에 업로드했습니다", manifest_s3_path)

        redshift = RedshiftSQLHook("redshift_conn_id")
        table = "source.source_event"
        redshift.run("BEGIN;")
        # redshift 중복 적재 방지
        # update 조건 created_at > observed_at
        redshift.run(f"""
            DELETE FROM {table}
            WHERE observed_at = '{ds_nodash}';
        """)
        redshift.run(f"""
            COPY {table} (
                area_code, area_name, event_name,
                event_period, event_place, event_x, event_y,
                is_paid, thumbnail_url, event_url,
                event_extra_detail, observed_at, created_at
            )
            FROM '{manifest_s3_path}'
            IAM_ROLE '{REDSHIFT_IAM_ROLE_ARN}'
            FORMAT AS PARQUET
            MANIFEST;
        """)
        # UTC로 들어간 created_at을 KST로 변환
        redshift.run(f"""
            UPDATE {table}
            SET created_at = CONVERT_TIMEZONE('UTC','Asia/Seoul', created_at)
            WHERE created_at::date = '{ds}';
        """)
        redshift.run("COMMIT;")
        log.info("%s 날짜의 이벤트 데이터를 Redshift에 성공적으로 적재했습니다.", ds)

    # dbt 실행
    run_dbt = BashOperator(
        task_id="run_dbt_command",
        # bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --select +fact_event",
    )

    pick = pick_event_file()
    run = run_event_glue
    load = load_to_redshift()
    dbt = run_dbt

    pick >> run >> load >> dbt


event_data_pipeline_with_glue = event_data_pipeline_with_glue()
