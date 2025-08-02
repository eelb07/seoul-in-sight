from __future__ import annotations

import json
import io
import pandas as pd
import pendulum
import logging

from datetime import datetime
from zoneinfo import ZoneInfo

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator

# --- 설정 변수 ---
BUCKET_NAME = Variable.get("BUCKET_NAME")
S3_PREFIX = Variable.get("S3_PREFIX")
S3_PQ_PREFIX_EVENT = Variable.get("S3_PQ_PREFIX_EVENT")
S3_PROCESSED_HISTORY_PREFIX = Variable.get("S3_PROCESSED_HISTORY_PREFIX")
REDSHIFT_CONN_ID = Variable.get("REDSHIFT_CONN_ID")
REDSHIFT_IAM_ROLE = Variable.get("REDSHIFT_IAM_ROLE_ARN")
DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR")

log = logging.getLogger(__name__)


# --- Airflow DAG 정의 ---
@dag(
    dag_id="dag_event",
    description="dag_event_data_pipeline",
    schedule="0 10 * * *",
    start_date=pendulum.datetime(2025, 7, 2, tz="Asia/Seoul"),
    catchup=True,
    tags=["seoul", "event", "ETL"],
    default_args={"retries": 1},
)
def event_data_pipeline():
    @task
    def extract_and_transform(**context) -> dict:
        logical_date = context["logical_date"].in_timezone("Asia/Seoul")
        observed_date = logical_date.strftime("%Y%m%d")
        # 00시 30분대(00시 30~09분)만 조회하도록 prefix
        prefix = f"{S3_PREFIX}/{observed_date}/003"

        current_timestamp = datetime.now(tz=ZoneInfo("Asia/Seoul")).isoformat()
        s3 = S3Hook(aws_conn_id="aws_default").get_conn()

        # history 불러오기
        history_key = f"{S3_PROCESSED_HISTORY_PREFIX}/event.json"
        history_dict = {}
        history_set = set()
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=history_key)
        if response.get("KeyCount", 0) > 0:
            obj = s3.get_object(Bucket=BUCKET_NAME, Key=history_key)
            history_dict = json.loads(obj["Body"].read().decode("utf-8"))
            for location_id, entries in history_dict.items():
                for entry in entries:
                    history_set.add((location_id, entry["observed_at"]))

        extracted_records = []
        object_list = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        for item in object_list.get("Contents", []):
            key = item["Key"]
            if not key.endswith(".json"):
                continue
            filename = key.rsplit("/", 1)[-1]
            if "_" not in filename:
                continue

            location_number = filename.split("_")[-1].replace(".json", "")
            # 이미 처리된 지역-날짜 조합 스킵
            if (location_number, observed_date) in history_set:
                continue

            # 처리 완료 표시 (이벤트 유무 관계없이)
            history_set.add((location_number, observed_date))
            history_dict.setdefault(location_number, []).append(
                {
                    "observed_at": observed_date,
                    "processed_at": current_timestamp,
                }
            )

            # JSON 읽기 및 이벤트 추출
            content = (
                s3.get_object(Bucket=BUCKET_NAME, Key=key)["Body"]
                .read()
                .decode("utf-8")
            )
            data = json.loads(content)
            events = data.get("EVENT_STTS", [])
            if not events:
                continue
            for event in events:
                extracted_records.append(
                    {
                        "area_code": data.get("AREA_CD"),
                        "area_name": data.get("AREA_NM"),
                        "event_name": event.get("EVENT_NM"),
                        "event_period": event.get("EVENT_PERIOD"),
                        "event_place": event.get("EVENT_PLACE"),
                        "event_x": float(event.get("EVENT_X"))
                        if event.get("EVENT_X")
                        else None,
                        "event_y": float(event.get("EVENT_Y"))
                        if event.get("EVENT_Y")
                        else None,
                        "is_paid": True
                        if event.get("PAY_YN") == "Y"
                        else False
                        if event.get("PAY_YN") == "N"
                        else None,
                        "thumbnail_url": event.get("THUMBNAIL"),
                        "event_url": event.get("URL"),
                        "event_extra_detail": event.get("EVENT_ETC_DETAIL"),
                        "observed_at": observed_date,
                        "created_at": current_timestamp,
                    }
                )

        log.info(f"[DEBUG] extract_and_transform 건수: {len(extracted_records)}")
        return {
            "event_records": extracted_records,
            "updated_processed_history": history_dict,
            "observed_date": observed_date,
        }

    @task
    def load_to_s3(pipeline_payload: dict) -> dict:
        event_records = pipeline_payload["event_records"]
        history_dict = pipeline_payload["updated_processed_history"]
        observed_date = pipeline_payload["observed_date"]

        parquet_key = f"{S3_PQ_PREFIX_EVENT}/{observed_date}.parquet"
        parquet_path = f"s3://{BUCKET_NAME}/{parquet_key}"
        s3_hook = S3Hook(aws_conn_id="aws_default")

        # 신규 레코드가 없는 경우
        if not event_records:
            # 이미 Parquet 파일이 존재하면, 그 경로만 반환
            if s3_hook.check_for_key(key=parquet_key, bucket_name=BUCKET_NAME):
                log.info(f"신규 이벤트 없음. 기존 Parquet 파일 사용: {parquet_key}")
                return {"parquet_path": parquet_path, "observed_date": observed_date}
            # 파일도 없으면 완전 스킵
            log.info("저장할 이벤트 데이터가 없고, 기존 Parquet 파일도 없습니다.")
            return {"parquet_path": "", "observed_date": observed_date}

        # 신규 레코드가 있는 경우: 기존 로직대로 Parquet 생성 & 업로드
        df = pd.DataFrame(event_records)
        df["observed_at"] = pd.to_datetime(df["observed_at"], format="%Y%m%d")
        df["created_at"] = (
            pd.to_datetime(df["created_at"])
            .dt.tz_convert("Asia/Seoul")
            .dt.tz_localize(None)
        )
        df = df[
            [
                "area_code",
                "area_name",
                "event_name",
                "event_period",
                "event_place",
                "event_x",
                "event_y",
                "is_paid",
                "thumbnail_url",
                "event_url",
                "event_extra_detail",
                "observed_at",
                "created_at",
            ]
        ]

        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)
        s3_hook.load_file_obj(
            file_obj=buffer,
            bucket_name=BUCKET_NAME,
            key=parquet_key,
            replace=True,
        )
        log.info(f"Parquet 업로드 완료: {parquet_key}")

        # history 업데이트
        s3_hook.get_conn().put_object(
            Bucket=BUCKET_NAME,
            Key=f"{S3_PROCESSED_HISTORY_PREFIX}/event.json",
            Body=json.dumps(history_dict, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        log.info(
            f"processed_history 파일 업데이트 완료: {S3_PROCESSED_HISTORY_PREFIX}/event.json"
        )

        return {"parquet_path": parquet_path, "observed_date": observed_date}

    @task
    def load_to_redshift(load_info: dict) -> None:
        parquet_path = load_info["parquet_path"]
        observed_date = load_info["observed_date"]

        if not parquet_path:
            log.info("Redshift 로드할 파일 없음")
            return
        hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)

        # 동일 observed_date 레코드가 있는지 확인
        existing = hook.get_first(
            "SELECT COUNT(1) FROM source.source_event WHERE observed_at = %s",
            parameters=[observed_date],
        )[0]
        if existing > 0:
            log.info(
                f"{observed_date} 데이터가 이미 {existing}건 존재하므로 적재를 건너뜁니다."
            )
            return

        # 없으면 COPY 실행, column명 명시
        hook.run(f"""
            COPY source.source_event (
                area_code, area_name, event_name,
                event_period, event_place,
                event_x, event_y, is_paid,
                thumbnail_url, event_url,
                event_extra_detail, observed_at, created_at
            )
            FROM '{parquet_path}'
            IAM_ROLE '{REDSHIFT_IAM_ROLE}'
            FORMAT AS PARQUET;
        """)
        log.info("Redshift 로드 완료")

    run_dbt = BashOperator(
        task_id="run_dbt_command",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --select +fact_event",
    )

    et = extract_and_transform()
    s3_info = load_to_s3(et)
    rd = load_to_redshift(s3_info)

    # task 의존성 명시
    rd >> run_dbt


# DAG 인스턴스화
event_data_pipeline = event_data_pipeline()
