import datetime
import decimal
import hashlib
import json
import logging
import os
import time
import traceback
from io import BytesIO

import boto3
import botocore.exceptions
import pandas as pd
import pendulum
import psutil # 시스템 유틸리티 (사용되지 않을 시 제거)
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable



BUCKET_NAME = Variable.get("BUCKET_NAME")
S3_PREFIX = Variable.get("S3_PREFIX")
S3_PQ_PREFIX_COMM = Variable.get("S3_PQ_PREFIX_COMM")
S3_PQ_PREFIX_RSB = Variable.get("S3_PQ_PREFIX_RSB")
S3_PROCESSED_HISTORY_PREFIX = Variable.get("S3_PROCESSED_HISTORY_PREFIX")
REDSHIFT_IAM_ROLE = Variable.get("REDSHIFT_IAM_ROLE_ARN")


log = logging.getLogger(__name__)

def generate_source_id(area_code: str, observed_at: str) -> str:
    raw = f"{area_code}_{observed_at}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]  # 32자리 고정 길이

def parse_int(val):
    try:
        return int(float(val)) if val not in [None, ""] else pd.NA
    except:
        return pd.NA

def parse_float(val):
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




default_args = {
    "owner": "seungalee", 
    "email": ["teamfirst.dag.alert@gmail.com"], # 알림을 받을 이메일 주소 목록
    "email_on_failure": True, 
}

@dag(
    dag_id="dag_commercial_v1",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2025, 7, 2, tz="Asia/Seoul"),
    catchup=False,
    doc_md="""
    # 상권 데이터 ETL 파이프라인
    - task1 :
    - task2 :
    """,
    tags=["seoul", "commercial", "ETL"],
    default_args=default_args
)
def commercial_data_pipeline():
    @task(task_id="extract_and_transfrom")
    def extract_and_transform(s3_client):

        process_start_time = pendulum.now(tz="Asia/Seoul")
        start_time_for_files = process_start_time.subtract(minutes=5)
        log.info(f"🔔{start_time_for_files} ~ {process_start_time} 사이의 raw_json 처리를 시작합니다.")

        # 처리해야할 전체 파일 경로를 정의 합니다. 
        files_to_process = []
        for i in range(5):
            file_time = start_time_for_files.add(minutes=i)
            file_time_HHmm = file_time.strftime("%H%M")

            for area_id in range(82):
                files_to_process.append({
                    'file_time': file_time,
                    'area_id': area_id,
                    'file_name': f"{file_time_HHmm}_{area_id}.json"
                })
        # S3에서 기존 처리 이력 로드
        processed_history_s3_key = f"{S3_PROCESSED_HISTORY_PREFIX}/processed_observations.json"
        processed_observed_at_set = set()
        processed_observed_at_dict = {}
        try:
            response = s3_client.get_object(Bucket=BUCKET_NAME, Key=processed_history_s3_key)
            processed_observed_at_dict = json.loads(response["Body"].read())

            for area_id, values in processed_observed_at_dict.items():
                area_id = int(area_id)
                for value in values:
                    observed_at = value['observed_at']
                    processed_observed_at_set.add((area_id, observed_at))  # set을 이용한 조회를 위해, json에서 이력을 모두 읽어와 (area_id, observed_at) 튜플로 구성된 set을 초기화 
            log.info(f"🔔 S3에서 {len(processed_observed_at_set)}개의 기존 처리 이력을 로드했습니다.")

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey": # 처리 이력 파일(json)이 없는 경우, 즉 첫 시작.
                log.warning(f"🚨 {processed_history_s3_key} 경로에 기존 처리 이력 파일이 없습니다. 새로 시작합니다.")
            else:
                raise 

        def is_processed(area_id: int, observed_at: str) -> bool:
            "해당 area_id와 observed_at 조합이 이미 처리 되었지 여부를 반환"
            return (area_id, observed_at) in processed_observed_at_set  
    
        source_commercial_data = []
        source_commercial_rsb_data = []

        for file_info in files_to_process:
            file_time = file_info['file_time']  # 2025-07-03 00:00:00+09:00
            area_id = file_info['area_id']
            file_name = file_info['file_name']

            key = f"{S3_PREFIX}/{file_time.strftime('%Y%m%d')}/{file_name}"  # 20250703/0000_{area_id}.json

            try:
                response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
                content = response["Body"].read()  
                raw_data = json.loads(content)
                raw_all_commercial_data = raw_data['LIVE_CMRCL_STTS']
                raw_observed_at = raw_all_commercial_data.get("CMRCL_TIME") # 20250702 2340
                
                try: # 20250702 2340 (STR) -> 2025-07-02 23:40:00 (DATETIME)
                    observed_at = pendulum.from_format(raw_observed_at, "YYYYMMDD HHmm", tz="Asia/Seoul").format("YYYY-MM-DD HH:mm:ss")
                except Exception as e:
                    log.error(f"🚨 해당 시각({raw_observed_at}) 파싱에 실패했습니다. - 오류: {e} ")
                    continue
                
                if is_processed(area_id=area_id, observed_at=observed_at):
                    log.info(f"⏭️ 해당 시각의 상권 데이터는 이미 처리 되었습니다. 스킵합니다. : {file_name} (area_id: {area_id}, observed_at: {observed_at})")
                    continue

                source_id = generate_source_id(raw_data.get("AREA_CD", ""), observed_at)

                source_commercial_data.append({
                    'source_id': source_id,
                    'area_code': str(raw_data.get("AREA_CD", "")),
                    'area_name': str(raw_data.get("AREA_NM", "")),
                    'congestion_level': str(raw_all_commercial_data.get("AREA_CMRCL_LVL", "")),                         # 장소 실시간 상권 현황 
                    'total_payment_count': parse_int(raw_all_commercial_data.get("AREA_SH_PAYMENT_CNT")),               # 장소 실시간 신한카드 결제 건수    -- INT32
                    'payment_amount_min': parse_int(raw_all_commercial_data.get("AREA_SH_PAYMENT_AMT_MIN")),            # 장소 실시간 신한카드 결제 최소값  -- INT32
                    'payment_amount_max': parse_int(raw_all_commercial_data.get("AREA_SH_PAYMENT_AMT_MAX")),            # 장소 실시간 신한카드 결제 최대값  -- INT32
                    'male_ratio': parse_float(raw_all_commercial_data.get("CMRCL_MALE_RATE")),                          # 남성 소비 비율
                    'female_ratio': parse_float(raw_all_commercial_data.get("CMRCL_FEMALE_RATE")),                      # 여성 소비 비율
                    'age_10s_ratio': parse_float(raw_all_commercial_data.get("CMRCL_10_RATE")),                         # 10대 소비 비율
                    'age_20s_ratio': parse_float(raw_all_commercial_data.get("CMRCL_20_RATE")),                         # 20대 소비 비율
                    'age_30s_ratio': parse_float(raw_all_commercial_data.get("CMRCL_30_RATE")),                         # 30대 소비 비율
                    'age_40s_ratio': parse_float(raw_all_commercial_data.get("CMRCL_40_RATE")),                         # 40대 소비 비율
                    'age_50s_ratio': parse_float(raw_all_commercial_data.get("CMRCL_50_RATE")),                         # 50대 소비 비율
                    'age_60s_ratio': parse_float(raw_all_commercial_data.get("CMRCL_60_RATE")),                         # 60대 소비 비율
                    'individual_consumer_ratio': parse_float(raw_all_commercial_data.get("CMRCL_PERSONAL_RATE")),       # 개인 소비 비율
                    'corporate_consumer_ratio': parse_float(raw_all_commercial_data.get("CMRCL_CORPORATION_RATE")),     # 법인 소비 비율
                    'observed_at': observed_at,                                                                         # 실시간 상권 업데이트 시간 
                    'created_at': pendulum.now("Asia/Seoul").to_datetime_string()                                       # 적재 시간
                })

                for idx, value in enumerate(raw_all_commercial_data.get("CMRCL_RSB", [])): # 여러 카테고리의 상권 데이터가 존재 
                    source_commercial_rsb_data.append({
                        # 'area_id': area_id,
                        'source_id': source_id,
                        'category_large': str(value.get("RSB_LRG_CTGR", "")),                                           # 업종 대분류
                        'category_medium': str(value.get("RSB_MID_CTGR", "")),                                          # 업종 중분류
                        'category_congestion_level': str(value.get("RSB_PAYMENT_LVL", "")),                             # 업종 실시간 상권 현황
                        'category_payment_count': parse_int(value.get("RSB_SH_PAYMENT_CNT")),                           # 업종 실시간 신한카드 결제 건수    -- INT32
                        'category_payment_min': parse_int(value.get("RSB_SH_PAYMENT_AMT_MIN")),                         # 업종 실시간 신한카드 결제 금액 최소값 -- INT32
                        'category_payment_max': parse_int(value.get("RSB_SH_PAYMENT_AMT_MAX")),                         # 업종 실시간 신한카드 결제 금액 최대값 -- INT32
                        'merchant_count': parse_int(value.get("RSB_MCT_CNT")),                                          # 업종 가맹점 수 -- INT32
                        'merchant_basis_month': str(value.get("RSB_MCT_TIME", "")),                                     # 업종 가맹점 수 업데이트 월    -- CHAR(6)
                        'observed_at': observed_at,                                                                     # 실시간 상권 현황 업데이트 시간
                        'created_at': pendulum.now("Asia/Seoul").to_datetime_string()                                   # 적재시간 
                    })
                log.info(f"🔔 {file_name}의 전처리를 완료했습니다.")


                # 성공적으로 처리된 데이터는 이력 set에 추가
                processed_observed_at_set.add((area_id, observed_at))
                # S3에 저장할 이력 딕셔너리에도 추가
                if str(area_id) not in processed_observed_at_dict:
                    processed_observed_at_dict[str(area_id)] = []
                processed_observed_at_dict[str(area_id)].append({
                    'observed_at': observed_at,
                    'processed_at': pendulum.now("Asia/Seoul").to_datetime_string()
                })

            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "NoSuchKey":
                    log.info(f"🚨 해당 시각({key})의 파일이 존재하지 않습니다. 건너뜁니다.")
                    continue
                else:
                    raise 

            
        return {
            'source_commercial_data' : source_commercial_data,
            'source_commercial_rsb_data' : source_commercial_rsb_data,
            'processed_observed_at_dict' : processed_observed_at_dict

        }
    
    @task(task_id="upload_parquet")
    def upload_parquet_to_s3(data_dict: dict, s3_client):
        commercial_data = data_dict['source_commercial_data']
        commercial_rsb_data = data_dict['source_commercial_rsb_data']

        current_process_time = pendulum.now("Asia/Seoul") 
        date_yyyymmdd = current_process_time.strftime('%Y%m%d')
        time_hhmm = current_process_time.strftime('%H%M') 

        saved_parquet_paths = {
            'commercial_parquet_path': None,
            'rsb_parquet_path': None
        }

        if commercial_data:
            # 딕셔너리 리스트를 PyArrow Table 생성을 위한 컬럼별 리스트(dict of lists)로 변환
            # 'source_id'는 Redshift에서 IDENTITY(1,1)이므로 Parquet 파일에 포함하지 않는다.
            
            columns_data = {
                'source_id': [],
                'area_code': [],
                'area_name': [],
                'congestion_level': [],
                'total_payment_count': [],
                'payment_amount_min': [],
                'payment_amount_max': [],
                'male_ratio': [],
                'female_ratio': [],
                'age_10s_ratio': [],
                'age_20s_ratio': [],
                'age_30s_ratio': [],
                'age_40s_ratio': [],
                'age_50s_ratio': [],
                'age_60s_ratio': [],
                'individual_consumer_ratio': [],
                'corporate_consumer_ratio': [],
                'observed_at': [],
                'created_at': []
            }
            
            # 데이터 타입 매핑 및 처리
            for row in commercial_data:
                # 문자열 컬럼
                columns_data['source_id'].append(str(row.get("source_id", "")))
                columns_data['area_code'].append(str(row.get("area_code", "")))
                columns_data['area_name'].append(str(row.get("area_name", "")))
                columns_data['congestion_level'].append(str(row.get("congestion_level", "")))
                
                columns_data['total_payment_count'].append(row.get("total_payment_count"))
                columns_data['payment_amount_min'].append(row.get("payment_amount_min"))
                columns_data['payment_amount_max'].append(row.get("payment_amount_max"))
                
                for col in [
                    "male_ratio", "female_ratio", "age_10s_ratio", "age_20s_ratio",
                    "age_30s_ratio", "age_40s_ratio", "age_50s_ratio", "age_60s_ratio", 
                    "individual_consumer_ratio", "corporate_consumer_ratio"
                ]:
                    val = row.get(col)
                    if pd.isna(val) or val is None: 
                        columns_data[col].append(None)
                    else:
                        columns_data[col].append(decimal.Decimal(str(round(float(val), 1))))

                try:
                    columns_data['observed_at'].append(pd.Timestamp(row.get("observed_at")))
                except (ValueError, TypeError):
                    columns_data['observed_at'].append(None)
                
                try:
                    columns_data['created_at'].append(pd.Timestamp(row.get("created_at")))
                except (ValueError, TypeError):
                    columns_data['created_at'].append(None)


            # PyArrow Schema 정의
            schema = pa.schema([
                pa.field('source_id', pa.string()),
                pa.field('area_code', pa.string()),
                pa.field('area_name', pa.string()),
                pa.field('congestion_level', pa.string()),
                pa.field('total_payment_count', pa.int32()),       # Redshift BIGINT에 매핑 -> int32
                pa.field('payment_amount_min', pa.int32()),        
                pa.field('payment_amount_max', pa.int32()),        
                pa.field('male_ratio', pa.decimal128(5, 2)),       
                pa.field('female_ratio', pa.decimal128(5, 2)),      
                pa.field('age_10s_ratio', pa.decimal128(5, 2)),     
                pa.field('age_20s_ratio', pa.decimal128(5, 2)),    
                pa.field('age_30s_ratio', pa.decimal128(5, 2)),    
                pa.field('age_40s_ratio', pa.decimal128(5, 2)),    
                pa.field('age_50s_ratio', pa.decimal128(5, 2)),    
                pa.field('age_60s_ratio', pa.decimal128(5, 2)),    
                pa.field('individual_consumer_ratio', pa.decimal128(5, 2)),
                pa.field('corporate_consumer_ratio', pa.decimal128(5, 2)), 
                pa.field('observed_at', pa.timestamp('s')),       
                pa.field('created_at', pa.timestamp('s'))         
            ])
            
            # PyArrow Table 생성 (from_pydict 사용)
            # from_pydict는 키-값 쌍의 딕셔너리에서 테이블을 생성함.
            table = pa.Table.from_pydict(columns_data, schema=schema)
                
            # Parquet 저장
            buffer_commercial = BytesIO()
            pq.write_table(table, buffer_commercial, compression='snappy')
            
            # S3 업로드
            s3_key_commercial = f"{S3_PQ_PREFIX_COMM}/{date_yyyymmdd}/{time_hhmm}.parquet"
            
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key_commercial,
                Body=buffer_commercial.getvalue(),
                ContentType='application/octet-stream'
            )
            print(f"🔔 상권 데이터를 저장 완료했습니다. : s3://{BUCKET_NAME}/{s3_key_commercial}")
            saved_parquet_paths['commercial_parquet_path'] = f"s3://{BUCKET_NAME}/{s3_key_commercial}"

        else:
            print("🚨 처리할 상권 일반 데이터가 없습니다.")


        if commercial_rsb_data:
            columns_data_rsb = {
                'source_id' : [],
                'category_large': [],
                'category_medium': [],
                'category_congestion_level' : [],
                'category_payment_count': [],
                'category_payment_min': [],
                'category_payment_max': [],
                'merchant_count': [],
                'merchant_basis_month' : [],
                'observed_at': [],
                'created_at': []
            }

            for row in commercial_rsb_data:
                columns_data_rsb['source_id'].append(str(row.get('source_id', '')))
                columns_data_rsb['category_large'].append(str(row.get('category_large', '')))
                columns_data_rsb['category_medium'].append(str(row.get('category_medium', '')))
                columns_data_rsb['category_congestion_level'].append(str(row.get('category_congestion_level', '')))
                
                columns_data_rsb['category_payment_count'].append(row.get('category_payment_count'))
                columns_data_rsb['category_payment_min'].append(row.get('category_payment_min'))
                columns_data_rsb['category_payment_max'].append(row.get('category_payment_max'))
                columns_data_rsb['merchant_count'].append(row.get('merchant_count'))
                columns_data_rsb['merchant_basis_month'].append(row.get('merchant_basis_month'))

                try:
                    columns_data_rsb['observed_at'].append(pd.Timestamp(row.get('observed_at')))
                except (ValueError, TypeError):
                    columns_data_rsb['observed_at'].append(None)
                
                try:
                    columns_data_rsb['created_at'].append(pd.Timestamp(row.get('created_at')))
                except (ValueError, TypeError):
                    columns_data_rsb['created_at'].append(None)

            # PyArrow Schema 정의
            schema_rsb = pa.schema([
                pa.field('source_id', pa.string()),
                pa.field('category_large', pa.string()),
                pa.field('category_medium', pa.string()),
                pa.field('category_congestion_level', pa.string()),
                pa.field('category_payment_count', pa.int32()), 
                pa.field('category_payment_min', pa.int32()),   
                pa.field('category_payment_max', pa.int32()),   
                pa.field('merchant_count', pa.int32()),         
                pa.field('merchant_basis_month', pa.string()),
                pa.field('observed_at', pa.timestamp('s')),   # Redshift TIMESTAMP -> pa.timestamp('ns')
                pa.field('created_at', pa.timestamp('s'))      # Redshift TIMESTAMP -> pa.timestamp('ns')
            ])

            # PyArrow Table 생성
            table_rsb = pa.Table.from_pydict(columns_data_rsb, schema=schema_rsb)

            # Parquet 파일로 저장
            buffer_rsb = BytesIO()
            pq.write_table(table_rsb, buffer_rsb, compression='snappy')
            
            # S3에 업로드
            s3_key_rsb = f"{S3_PQ_PREFIX_RSB}/{date_yyyymmdd}/{time_hhmm}.parquet"
            
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key_rsb,
                Body=buffer_rsb.getvalue(),
                ContentType='application/octet-stream'
            )
            print(f"🔔 상권 카테고리별 데이터를 저장 완료했습니다. : s3://{BUCKET_NAME}/{s3_key_rsb}")
            saved_parquet_paths['rsb_parquet_path'] = f"s3://{BUCKET_NAME}/{s3_key_rsb}"
        else:
            print("🚨 처리할 상권 RSB 데이터가 없습니다.")

        processed_history_s3_key = f"{S3_PROCESSED_HISTORY_PREFIX}/commercial.json"
        try:
            upload_processed_history_to_s3(
                s3_client=s3_client,
                bucket_name=BUCKET_NAME,
                s3_key=processed_history_s3_key,
                processed_history_data= data_dict['processed_observed_at_dict']
            )
        except Exception as e:
            print(f"❌ 최종 처리 이력 업로드 중 오류 발생: {e}")
            raise

        return {
            'processed_observed_at_dict' :  data_dict['processed_observed_at_dict'], 
            's3_parquet_paths': saved_parquet_paths
        }


    @task(task_id="load_to_redshift") 
    def load_to_redshift(saved):
        """
        S3에서 Parquet 파일을 Redshift 테이블로 COPY INTO 명령을 사용하여 로드합니다.
        """
        s3_parquet_paths = saved['s3_parquet_paths']
        commercial_parquet_path = s3_parquet_paths.get('commercial_parquet_path')
        rsb_parquet_path = s3_parquet_paths.get('rsb_parquet_path')

        if not commercial_parquet_path and not rsb_parquet_path:
            print("🚨 Redshift로 로드할 Parquet 파일 경로가 없습니다.")
            return

            # Airflow Connection에서 Redshift 연결 정보 가져오기
        conn_obj = None
        conn_obj = BaseHook.get_connection("redshift_conn_id")

        conn = None
        try:
            conn = psycopg2.connect(
                dbname=conn_obj.schema,
                user=conn_obj.login,
                password=conn_obj.password,
                host=conn_obj.host,
                port=int(conn_obj.port) 
            )
            conn.autocommit = True 
            log.info("Redshift에 성공적으로 연결되었습니다.")

            with conn.cursor() as cur:
                if commercial_parquet_path:
                    commercial_table_name = "source.source_commercial" 
                    print(f"🔄 Redshift 테이블 '{commercial_table_name}'에 데이터 로드 시작...")

                    copy_commercial_sql = f"""
                    COPY {commercial_table_name} (
                        source_id,
                        area_code,
                        area_name,
                        congestion_level,
                        total_payment_count,
                        payment_amount_min,
                        payment_amount_max,
                        male_ratio,
                        female_ratio,
                        age_10s_ratio,
                        age_20s_ratio,
                        age_30s_ratio,
                        age_40s_ratio,
                        age_50s_ratio,
                        age_60s_ratio,
                        individual_consumer_ratio,
                        corporate_consumer_ratio,
                        observed_at,
                        created_at
                    )
                    FROM '{commercial_parquet_path}'
                    IAM_ROLE '{REDSHIFT_IAM_ROLE}'
                    FORMAT AS PARQUET;
                    """
                    cur.execute(copy_commercial_sql)
                    print(f"✅ 상권 데이터가 Redshift 테이블 '{commercial_table_name}'에 성공적으로 로드되었습니다.")

                if rsb_parquet_path:
                    rsb_table_name = "source.source_commercial_rsb" 
                    print(f"🔄 Redshift 테이블 '{rsb_table_name}'에 데이터 로드 시작...")
                    copy_rsb_sql = f"""
                    COPY {rsb_table_name} (
                    source_id,
                    category_large,
                    category_medium,
                    category_congestion_level,
                    category_payment_count,
                    category_payment_min,
                    category_payment_max,
                    merchant_count,
                    merchant_basis_month,
                    observed_at,
                    created_at
                )
                    FROM '{rsb_parquet_path}'
                    IAM_ROLE '{REDSHIFT_IAM_ROLE}'
                    FORMAT AS PARQUET;
                    """
                    cur.execute(copy_rsb_sql)
                    print(f"✅ 상권 카테고리별 데이터가 Redshift 테이블 '{rsb_table_name}'에 성공적으로 로드되었습니다.")

        except Exception as e:
            print(f"❌ Redshift에 데이터 로드 중 오류 발생: {e}")
            raise
        finally:
            if conn:
                conn.close()
                print("🗄️ Redshift 연결이 닫혔습니다.")


    try:
        conn = BaseHook.get_connection("aws_default")
        session = boto3.Session(
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
            region_name=json.loads(conn.extra)["region_name"]
        )
        s3 = session.client("s3")
        logging.info("S3 클라이언트 초기화 완료.")
    except Exception as e:
        logging.critical(f"S3 클라이언트 초기화 실패: {e}")
        return
    
    extracted_data = extract_and_transform(s3_client=s3)
    saved = upload_parquet_to_s3(extracted_data, s3)
    load_to_redshift(saved)


commercial_pipeline_dag = commercial_data_pipeline() 
