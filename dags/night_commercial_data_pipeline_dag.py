import logging
import hashlib
import json
from io import BytesIO
import decimal

import pendulum
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import botocore.exceptions

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

# --- 설정 변수 ---
BUCKET_NAME = Variable.get("BUCKET_NAME")
S3_PREFIX = Variable.get("S3_PREFIX")
S3_PQ_PREFIX_COMM = Variable.get("S3_PQ_PREFIX_COMM")
S3_PQ_PREFIX_RSB = Variable.get("S3_PQ_PREFIX_RSB")
S3_PROCESSED_HISTORY_PREFIX = Variable.get("S3_PROCESSED_HISTORY_PREFIX")
REDSHIFT_IAM_ROLE = Variable.get("REDSHIFT_IAM_ROLE_ARN")
DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR")

# 필수 변수 검증
required_vars = [
    BUCKET_NAME, S3_PREFIX, S3_PQ_PREFIX_COMM, 
    S3_PQ_PREFIX_RSB, S3_PROCESSED_HISTORY_PREFIX, REDSHIFT_IAM_ROLE, DBT_PROJECT_DIR
]
if not all(required_vars):
    raise ValueError("필수 Airflow Variables가 설정되지 않았습니다.")

log = logging.getLogger(__name__)

# --- 헬퍼 함수 ---

def generate_source_id(area_code: str, observed_at: str) -> str:
    """
    고정 길이 MD5 해시를 소스 ID로 생성합니다.

    Args:
        area_code (str): 지역 코드.
        observed_at (str): 관측 시간.

    Returns:
        str: 32자 MD5 해시 문자열.
    """
    raw = f"{area_code}_{observed_at}"
    return hashlib.md5(raw.encode()).hexdigest()

def parse_int(val):
    """
    값을 정수로 파싱하고, None, 빈 문자열 및 float 변환을 처리합니다.
    파싱할 수 없는 값은 pd.NA를 반환합니다.
    """
    try:
        return int(float(val)) if val not in [None, ""] else pd.NA
    except (ValueError, TypeError):
        return pd.NA

def parse_float(val):
    """
    값을 float으로 파싱합니다. 파싱할 수 없는 값은 None을 반환합니다.
    """
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def upload_processed_history_to_s3(
    s3_client,
    bucket_name: str,
    s3_key: str,
    processed_history_data: dict # 업로드할 processed_observed_at_dict 데이터
):
    """
    업데이트된 처리 이력 딕셔너리 (processed_observed_at_dict)를 S3에 JSON 파일로 업로드합니다.

    Args:
        s3_client: 초기화된 boto3 S3 클라이언트 객체.
        bucket_name (str): S3 버킷 이름.
        s3_key (str): 처리 이력 파일이 저장될 S3 키 (예: "history/processed_observations.json").
        processed_history_data (dict): area_id 별로 그룹화된 처리 이력 데이터 (processed_observed_at_dict).
    """
    try:
        updated_content_json_string = json.dumps(processed_history_data, indent=4, ensure_ascii=False)

        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=updated_content_json_string.encode('utf-8'),
            ContentType='application/json'
        )

        total_records_count = sum(len(observations) for observations in processed_history_data.values())
        log.info(f"✅ S3에 {total_records_count}개의 처리 이력을 성공적으로 업데이트했습니다: s3://{bucket_name}/{s3_key}")

    except Exception as e:
        log.error(f"❌ 처리 이력 파일을 S3에 저장하는 데 실패했습니다 (s3://{bucket_name}/{s3_key}): {e}")
        raise

# --- Airflow DAG 정의 ---

default_args = {
    "owner": "seungalee",
    "email": ["teamfirst.dag.alert@gmail.com"], # 알림을 받을 이메일 주소 목록 ( 향후 적용 가능. 로컬에선 안됨)
    "email_on_failure": True,
}

@dag(
    dag_id="dag_commercial_night",
    schedule="30 9 * * *", 
    start_date=pendulum.datetime(2025, 7, 13, tz="Asia/Seoul"),
    catchup=False,
    doc_md="""
    # 상권 데이터 ETL 파이프라인
    - **추출 및 변환**: S3에서 원시 JSON 데이터를 추출, 변환하고 이미 처리된 레코드를 필터링합니다.
    - **Parquet 업로드**: 처리된 데이터를 Parquet 파일로 S3에 업로드합니다.
    - **Redshift 로드**: S3의 Parquet 파일을 Redshift 테이블로 로드합니다.
    - **dbt 모델 실행**: 증분 로딩 및 데이터 변환을 위해 dbt 모델을 실행합니다.
    """,
    tags=["seoul", "commercial", "ETL"],
    default_args=default_args
)
def commercial_data_pipeline():

    @task()
    def extract_and_transform(**context):
        """
        S3에서 원시 상권 데이터를 추출하고 변환하며, S3의 이력 파일을 기반으로
        이미 처리된 레코드를 필터링합니다.
        """

        return 

    # 데이터 추출 및 전처리
    extract_and_transform()

    
commercial_pipeline_dag = commercial_data_pipeline()