import json
import logging
import pandas as pd
import pendulum
import boto3
import botocore.exceptions
import io
from typing import Optional, List
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from collections import defaultdict

logger = logging.getLogger()
# S3_BUCKET_NAME = 'de6-seoul-data1'
S3_BUCKET_NAME = "de6-team1-bucket"


def get_s3_client(conn_id="aws_conn_id"):
    conn = BaseHook.get_connection(conn_id)
    session = boto3.Session(
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name=conn.extra_dejson.get("region_name", "ap-northeast-2"),
    )
    return session.client("s3")


def list_s3_keys(bucket_name: str, prefix: str, conn_id="aws_conn_id"):
    s3 = get_s3_client(conn_id)
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    keys = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def read_from_s3(bucket_name: str, key: str, conn_id="aws_conn_id"):
    s3 = get_s3_client(conn_id)
    response = s3.get_object(Bucket=bucket_name, Key=key)
    return response["Body"].read().decode("utf-8")


def upload_to_s3(bucket_name: str, key: str, data, conn_id="aws_conn_id"):
    s3 = get_s3_client(conn_id)
    s3.put_object(Bucket=bucket_name, Key=key, Body=data)


def get_latest_observed_at(
    bucket_name: str, date_prefix: str, area_code: int, conn_id="aws_conn_id"
) -> Optional[str]:
    prefix = f"raw-json/{date_prefix}/"
    keys = list_s3_keys(bucket_name, prefix, conn_id)
    s3 = get_s3_client(conn_id)

    latest_observed_at = None
    for key in keys:
        if not key.endswith(f"_{area_code}.json"):
            continue

        tagging = s3.get_object_tagging(Bucket=bucket_name, Key=key)
        tags = {tag["Key"]: tag["Value"] for tag in tagging.get("TagSet", [])}
        if tags.get("population") != "true":
            continue

        raw_json = read_from_s3(bucket_name, key, conn_id)
        raw_data = json.loads(raw_json)
        citydata = raw_data.get("LIVE_PPLTN_STTS", [])
        if not citydata:
            continue

        observed_at = (
            citydata[0].get("PPLTN_TIME", "0000").replace(":", "").replace(" ", "")
        )

        if latest_observed_at is None or observed_at > latest_observed_at:
            latest_observed_at = observed_at

    return latest_observed_at


def update_population_tag(bucket_name: str, key: str, conn_id="aws_conn_id") -> None:
    s3 = get_s3_client(conn_id)
    tagging = s3.get_object_tagging(Bucket=bucket_name, Key=key)
    existing_tags = {tag["Key"]: tag["Value"] for tag in tagging.get("TagSet", [])}
    existing_tags["population"] = "true"

    new_tag_set = [{"Key": k, "Value": v} for k, v in existing_tags.items()]
    s3.put_object_tagging(Bucket=bucket_name, Key=key, Tagging={"TagSet": new_tag_set})


@task
def read_and_transform_all(**context) -> List[str]:
    exec_date = (
        pendulum.parse(context["logical_date"])
        if isinstance(context["logical_date"], str)
        else context["logical_date"]
    )
    process_start_time = exec_date
    start_time_for_files = process_start_time.subtract(minutes=5)
    date_prefix = process_start_time.format("YYYYMMDD")

    files_to_process = []
    for i in range(5):
        file_time = start_time_for_files.add(minutes=i)
        file_time_HHmm = file_time.format("HHmm")
        for area_id in [7, 8, 9]:
            file_name = f"{file_time_HHmm}_{area_id}.json"
            files_to_process.append(f"raw-json/{date_prefix}/{file_name}")

    s3 = get_s3_client()
    all_s3_keys = list_s3_keys(S3_BUCKET_NAME, f"raw-json/{date_prefix}/")
    area_file_map = defaultdict(list)

    for key in files_to_process:
        if key not in all_s3_keys:
            logger.warning(f"S3 key not found: {key}")
            continue

        try:
            raw_json = read_from_s3(S3_BUCKET_NAME, key)
            raw_data = json.loads(raw_json)
            tagging = s3.get_object_tagging(Bucket=S3_BUCKET_NAME, Key=key)
            tags = {tag["Key"]: tag["Value"] for tag in tagging.get("TagSet", [])}
            is_population = tags.get("population") == "true"

            citydata = raw_data.get("LIVE_PPLTN_STTS", [])
            if not citydata:
                continue

            observed_at = (
                citydata[0]
                .get("PPLTN_TIME", "unknown_time")
                .replace(":", "")
                .replace(" ", "")
            )

            area_id = int(key.split("_")[-1].split(".")[0])

            area_file_map[area_id].append(
                {
                    "key": key,
                    "observed_at": observed_at,
                    "is_population": is_population,
                    "data": citydata,
                }
            )
            # ex)
            # area_file_map[7].append({
            #     'key': 'raw-json/20250703/0005_7.json',
            #     'observed_at': '2345',
            #     'is_population': False,
            #     'data': [{"AREA_CD": "POI007", "PPLTN_TIME": "2025-07-02 23:45:00", "AREA_NM": "홍대 관광특구", ...}]
            # })

            logger.info(
                f"[DEBUG] Found file: {key}, population={tags.get('population')}, observed_at={observed_at}"
            )

        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.warning(f"S3 key not found during read: {key}")
                continue
            else:
                raise

    processed_keys = []

    for area_id, files in area_file_map.items():
        latest_observed_at = get_latest_observed_at(
            S3_BUCKET_NAME, date_prefix, area_id
        )
        logger.info(
            f"[DEBUG] Latest observed_at for area_code={area_id} is {latest_observed_at}"
        )

        obs_group = defaultdict(list)
        for f in files:
            if not f["is_population"]:
                obs_group[f["observed_at"]].append(
                    f
                )  # population tag가 false인 json의 observed_at
                # ex)
                # {
                #     '2345': [
                #         { 'key': 'raw-json/20250703/0005_7.json', ... },
                #         { 'key': 'raw-json/20250703/0010_7.json', ... }
                #     ]
                # }

        for observed_at, group in obs_group.items():
            # population 태그만 수정
            for f in group:
                update_population_tag(S3_BUCKET_NAME, f["key"])

            if (
                observed_at == latest_observed_at
            ):  # population tag가 true인 제일 최신 observed_at과 비교해서 같으면 이미 처리됐으므로 skip
                continue

            group.sort(key=lambda x: x["key"], reverse=True)  # 제일 최근 수집된 json

            # 최신 파일 parquet 저장
            latest_file = group[0]
            df = pd.json_normalize(latest_file["data"])
            df.rename(
                columns={
                    "AREA_CD": "area_code",
                    "AREA_NM": "area_name",
                    "AREA_CONGEST_LVL": "congestion_label",
                    "AREA_CONGEST_MSG": "congestion_message",
                    "AREA_PPLTN_MIN": "population_min",
                    "AREA_PPLTN_MAX": "population_max",
                    "MALE_PPLTN_RATE": "male_population_ratio",
                    "FEMALE_PPLTN_RATE": "female_population_ratio",
                    "PPLTN_RATE_0": "age_0s_ratio",
                    "PPLTN_RATE_10": "age_10s_ratio",
                    "PPLTN_RATE_20": "age_20s_ratio",
                    "PPLTN_RATE_30": "age_30s_ratio",
                    "PPLTN_RATE_40": "age_40s_ratio",
                    "PPLTN_RATE_50": "age_50s_ratio",
                    "PPLTN_RATE_60": "age_60s_ratio",
                    "PPLTN_RATE_70": "age_70s_ratio",
                    "RESNT_PPLTN_RATE": "resident_ratio",
                    "NON_RESNT_PPLTN_RATE": "non_resident_ratio",
                    "REPLACE_YN": "is_replaced",
                    "PPLTN_TIME": "observed_at",
                },
                inplace=True,
            )

            df.drop(columns=["FCST_YN", "FCST_PPLTN"], inplace=True)
            df["created_at"] = pd.Timestamp.now(tz="Asia/Seoul").tz_localize(None)

            df = df.astype(
                {
                    "area_code": "string",
                    "area_name": "string",
                    "congestion_label": "string",
                    "congestion_message": "string",
                    "population_min": "Int32",
                    "population_max": "Int32",
                    "male_population_ratio": "float32",
                    "female_population_ratio": "float32",
                    "age_0s_ratio": "float32",
                    "age_10s_ratio": "float32",
                    "age_20s_ratio": "float32",
                    "age_30s_ratio": "float32",
                    "age_40s_ratio": "float32",
                    "age_50s_ratio": "float32",
                    "age_60s_ratio": "float32",
                    "age_70s_ratio": "float32",
                    "resident_ratio": "float32",
                    "non_resident_ratio": "float32",
                    "is_replaced": "bool",
                    "observed_at": "datetime64[ns]",  # 또는 "datetime64[ns]" 도 가능
                    "created_at": "datetime64[ns]",
                }
            )
            # logger.info(f"Parquet columns: {df.dtypes}")

            # logger.info(f"Final columns before saving parquet: {df.columns.tolist()}")

            parquet_key = f"processed-data/area_population/{date_prefix}/{observed_at}_{area_id}.parquet"
            parquet_data = df.to_parquet(index=False, engine="pyarrow")
            upload_to_s3(S3_BUCKET_NAME, parquet_key, parquet_data)
            processed_keys.append(parquet_key)

            # 덜 최신 파일 parquet 저장 여부 판단
            for old_file in group[1:]:
                if old_file["observed_at"] == latest_observed_at:
                    continue  # 이미 처리된 시간과 같으면 skip

                df_old = pd.json_normalize(old_file["data"])
                df_old.rename(columns=df.columns, inplace=True)
                df_old["created_at"] = pd.Timestamp.now(tz="Asia/Seoul").tz_localize(
                    None
                )
                parquet_key_old = f"processed-data/area_population/{date_prefix}/{old_file['observed_at']}_{area_id}.parquet"
                parquet_data_old = df_old.to_parquet(index=False, engine="pyarrow")
                upload_to_s3(S3_BUCKET_NAME, parquet_key_old, parquet_data_old)
                processed_keys.append(parquet_key_old)

    return processed_keys


@task
def merge_parquet_files(s3_keys: List[str]) -> str:
    s3 = get_s3_client()
    logger.info(f"s3_keys: {s3_keys}")

    dfs = []
    for key in s3_keys:
        response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
        parquet_data = response["Body"].read()
        buffer = io.BytesIO(parquet_data)
        df = pd.read_parquet(buffer, engine="pyarrow")
        logger.info(f"[DEBUG] {key} rows: {len(df)}")
        dfs.append(df)

    combined_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"[DEBUG] Combined rows: {len(combined_df)}")

    merged_key = f"processed-data/population/{pendulum.now('Asia/Seoul').format('YYYYMMDD_HHmmss')}.parquet"
    buffer = io.BytesIO()
    combined_df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    logger.info(f"[DEBUG] Parquet buffer size: {buffer.getbuffer().nbytes} bytes")

    s3.put_object(Bucket=S3_BUCKET_NAME, Key=merged_key, Body=buffer.getvalue())

    response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=merged_key)
    parquet_data = response["Body"].read()
    buffer = io.BytesIO(parquet_data)
    df_check = pd.read_parquet(buffer, engine="pyarrow")
    logger.info(f"[DEBUG] Final saved parquet rows: {len(df_check)}")

    return merged_key


@task
def create_redshift_table():
    create_table = PostgresOperator(
        task_id="create_source_area_population_table",
        postgres_conn_id="redshift_dev_db",
        sql="""
            CREATE TABLE IF NOT EXISTS public.source_area_population (
                source_id BIGINT IDENTITY(1,1),
                area_name VARCHAR(100),
                area_code VARCHAR(20),
                congestion_label VARCHAR(20),
                congestion_message TEXT,
                population_min INT,
                population_max INT,
                male_population_ratio FLOAT4,
                female_population_ratio FLOAT4,
                age_0s_ratio FLOAT4,
                age_10s_ratio FLOAT4,
                age_20s_ratio FLOAT4,
                age_30s_ratio FLOAT4,
                age_40s_ratio FLOAT4,
                age_50s_ratio FLOAT4,
                age_60s_ratio FLOAT4,
                age_70s_ratio FLOAT4,
                resident_ratio FLOAT4,
                non_resident_ratio FLOAT4,
                is_replaced BOOLEAN,
                observed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT GETDATE()
            );
        """,
    )

    return create_table.execute({})


@task
def load_to_redshift(merged_key: str):
    copy_task = S3ToRedshiftOperator(
        task_id="load_merged_data_from_s3",
        redshift_conn_id="redshift_dev_db",
        aws_conn_id="aws_conn_id",
        schema="public",
        table="source_area_population",
        copy_options=["FORMAT AS PARQUET"],
        s3_bucket=S3_BUCKET_NAME,
        s3_key=merged_key,
    )

    return copy_task.execute({})


@dag(
    dag_id="test_parquet_backup_pipeline",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2025, 7, 3, 0, 5, tz="Asia/Seoul"),
    end_date=pendulum.datetime(2025, 7, 3, 0, 30, tz="Asia/Seoul"),
    catchup=True,
    tags=["test", "s3", "parquet"],
    default_args={"owner": "hyeonuk"},
)
def test_parquet_backup_pipeline():
    s3_keys = read_and_transform_all()
    merged_key = merge_parquet_files(s3_keys)
    create_table_task = create_redshift_table()
    load_task = load_to_redshift(merged_key)

    create_table_task >> load_task


dag_instance = test_parquet_backup_pipeline()
