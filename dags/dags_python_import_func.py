from airflow.models.dag import DAG
import datetime
import pendulum # Datetime 타입을 쉽쉽게 사용할 수 있게
from airflow.operators.python import PythonOperator
from common.common_func import get_sftp  # common_func.py에서 import한 함수, root 기준
import random
with DAG(
    dag_id="dags_python_import_func", # DAG 이름 (Python 파일명과는 상관 없음, 일치시키는 게 좋음.)
    schedule="30 6 * * *", # 5개 항목: 분,시, 일,월,요일 => 언제 작동할지 설정정
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"), # DAG 시작 날짜
    catchup=False, # False: DAG이 시작된 시점부터 스케줄에 맞춰서 실행, True: DAG이 시작된 시점부터 이전까지의 모든 스케줄을 실행 == 누락 시간 반영 여부
    # dagrun_timeout=datetime.timedelta(minutes=60), # DAG이 실행되는 최대 시간
    # tags=["example", "example2"], # DAG 태그
    # params={"example_key": "example_value"}, # Task에 전달할 공통 파라미터
) as dag:
    task_get_sftp=PythonOperator(
        task_id="task_get_sftp", # Task ID, graph 상에서 보여지는 이름
        python_callable=get_sftp # 어떤 함수를 실행시킬지
    )

    py_t1