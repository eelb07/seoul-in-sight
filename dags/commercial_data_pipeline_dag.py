from __future__ import annotations
import pendulum
import logging 
from airflow.decorators import dag, task

# 로거 설정 (DAG 레벨)
log = logging.getLogger(__name__)
default_args = {
    "owner": "seungalee", 
    "email": ["teamfirst.dag.alert@gmail.com"], # 알림을 받을 이메일 주소 목록
    "email_on_failure": True, 
}

@dag(
    dag_id="dag_commercial_failure_test",
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
    @task(task_id="failure_mail_test")
    def alert_test():
        """
        이메일 알림 테스트를 위해 의도적으로 실패하는 태스크
        - Divide by zero 오류를 발생
        """
        log.info("🚨 failure_mail_test 태스크가 시작되었습니다. (이 태스크는 의도적으로 실패할 예정입니다.)")
        try:
            result = 1 / 0
            log.info(f"이 메시지는 보이지 않을 것입니다: {result}")
        except ZeroDivisionError as e:
            log.error(f"❌ 예상된 오류 발생: {e}")
            log.error("이 태스크는 이메일 알림 테스트를 위해 실패하도록 설정되었습니다.")
            raise e
        log.info("이 메시지는 실행되지 않을 것입니다.")

    alert_test()

commercial_pipeline_dag = commercial_data_pipeline() 