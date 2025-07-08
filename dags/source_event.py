from __future__ import annotations

import json
import io
import hashlib
import pandas as pd
import pendulum

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

BUCKET: str = "de6-team1-bucket"
SECTION: str = "event"
AWS_CONN_ID: str = "aws_default"


# DAG 정의: 매일 오전 10시 실행
@dag(
    dag_id="source_event",
    description="Silver - S3의 JSON을 읽어 event 전처리 후 Parquet 저장",
    schedule="0 10 * * *",  # 매일 오전 10시
    # schedule=None,
    start_date=pendulum.datetime(2025, 7, 2, tz="Asia/Seoul"),
    catchup=True,
    tags=["silver", "event"],
    default_args={"retries": 1},
)
def source_event_dag():
    @task
    def extract_event(**context) -> list[dict]:
        """
        S3의 raw-json/{날짜}/ 경로에서 JSON을 읽고
        EVENT_STTS 데이터만 추출하여 list[dict]로 반환
        """
        logical_date = context["logical_date"]
        logical_date = logical_date.in_timezone("Asia/Seoul")
        today = logical_date.strftime("%Y%m%d")

        prefix = f"raw-json/{today}/00"  # 00시 데이터만 조회

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3_client = s3_hook.get_conn()

        extracted: list[dict] = []
        seen_locations: set = set()

        response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        for obj in response.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue

            # ex: raw-json/20250701/2345_7.json
            filename = key.split("/")[-1]
            if "_" not in filename:
                continue

            location_number = filename.split("_")[-1].replace(".json", "")
            if location_number in seen_locations:
                continue

            # 적재된 number는 pass
            seen_locations.add(location_number)

            try:
                content = s3_hook.read_key(key=key, bucket_name=BUCKET)
                data = json.loads(content)

                area_name = data.get("AREA_NM")
                area_code = data.get("AREA_CD")
                events = data.get("EVENT_STTS", [])

                if not events:
                    continue

                for event in events:
                    event_name = event.get("EVENT_NM")
                    created_at = logical_date.isoformat()

                    raw_key = f"{today}_{area_code}_{event_name}"
                    source_id = hashlib.md5(raw_key.encode()).hexdigest()

                    row = {
                        "source_id": source_id,
                        "area_code": area_code,
                        "area_name": area_name,
                        "event_name": event_name,
                        "event_period": event.get("EVENT_PERIOD"),
                        "event_place": event.get("EVENT_PLACE"),
                        "event_x": float(event.get("EVENT_X"))
                        if event.get("EVENT_X")
                        else None,
                        "event_y": float(event.get("EVENT_Y"))
                        if event.get("EVENT_Y")
                        else None,
                        "is_paid": True
                        if event.get("PAY_YN") == "Y"  # 데이터 체크 시 Y값 없음
                        else False
                        if event.get("PAY_YN") == "N"
                        else None,
                        "thumbnail_url": event.get("THUMBNAIL"),
                        "event_url": event.get("URL"),
                        "event_extra_detail": event.get("EVENT_ETC_DETAIL"),
                        "created_at": created_at,
                    }
                    extracted.append(row)

            except Exception as e:
                print(f"오류: {key} - {e}")
                continue

        return extracted

    @task
    def store_event(data: list[dict], **context) -> None:
        """
        추출된 이벤트 데이터를 DataFrame으로 변환하고,
        Parquet 포맷으로 저장하여 S3에 업로드
        """
        logical_date = context["logical_date"]
        logical_date = logical_date.in_timezone("Asia/Seoul")
        today = logical_date.strftime("%Y%m%d")

        if not data:
            print("저장할 이벤트 데이터가 없습니다.")
            return

        df = pd.DataFrame(data)

        columns_order = [
            "source_id",
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
            "created_at",
        ]
        df = df[columns_order]

        df["created_at"] = pd.to_datetime(df["created_at"])
        df["created_at"] = (
            df["created_at"].dt.tz_convert("Asia/Seoul").dt.tz_localize(None)
        )

        today = logical_date.strftime("%Y%m%d")
        output_key = f"processed-data/{SECTION}/{today}.parquet"

        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3_hook.load_file_obj(
            file_obj=buffer,
            key=output_key,
            bucket_name=BUCKET,
            replace=True,
        )

        print(f"저장 완료: {output_key}")

    # extract → store
    extract_task = extract_event()
    store_task = store_event(extract_task)

    extract_task >> store_task


source_event_dag()
