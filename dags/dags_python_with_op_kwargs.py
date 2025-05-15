from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import regist2
with DAG(
    dag_id="dags_python_with_op_kwargs", # DAG 이름 (Python 파일명과는 상관 없음, 일치시키는 게 좋음.)
    schedule="30 6 * * *", # 5개 항목: 분,시, 일,월,요일 => 언제 작동할지 설정
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"), # DAG 시작 날짜
    catchup=False, # False: DAG이 시작된 시점부터 스케줄에 맞춰서 실행, True: DAG이 시작된 시점부터 이전까지의 모든 스케줄을 실행 == 누락 시간 반영 여부    
)as dag:
    
    regist2_t1=PythonOperator(
        task_id="regist2_t1", # Task ID, graph 상에서 보여지는 이름
        python_callable=regist2, # 어떤 함수를 실행시킬지
        op_args=["geonhui",'man','kr','seoul']
        op_kwargs={'email':'kunhei86@gmail.com','phone':'010'}, # 함수에 전달할 인자
    )