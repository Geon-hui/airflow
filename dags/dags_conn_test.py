from airflow.models.dag import DAG
import datetime
import pendulum 
from airflow.operators.bash import EmptyOperator

with DAG(
    dag_id="dags_conn_test", # DAG 이름 (Python 파일명과는 상관 없음, 일치시키는 게 좋음.)
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"), # DAG 시작 날짜
    catchup=False, # False: DAG이 시작된 시점부터 스케줄에 맞춰서 실행, True: DAG이 시작된 시점부터 이전까지의 모든 스케줄을 실행 == 누락 시간 반영 여부
) as dag:
    t1=EmptyOperator(
        task_id="t1",
    )
    t2=EmptyOperator(
        task_id="t2",
    )
    t3=EmptyOperator(
        task_id="t3",
    )

    t4=EmptyOperator(
        task_id="t4",
    )
    t5=EmptyOperator(
        task_id="t5",
    )
    t6=EmptyOperator(
        task_id="t6",
    )
    t7=EmptyOperator(
        task_id="t7",
    )
    t8=EmptyOperator(
        task_id="t8",
    )
    t1>>[t2,t3] >>t4
    t5>>t4
    [t4,t7]>>t6 >>t8