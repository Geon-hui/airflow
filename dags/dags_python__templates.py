from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_python__templates",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025, 5, 10, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    def python_function1(start_date, end_date,**kwargs): # op_kwargs를 이용해 명시적인 key-value 쌍으로 전달 => 출력 확인
        print(start_date)
        print(end_date)

        python_t1= PythonOperator(
            task_id='python_task',
            python_callable=python_function1,
            op_kwargs={'start_date': '{{data_interval_start | ds}}', 'end_date': '{{data_interval_end | ds}}'}
        )

    @task(task_id='python_t2') # op_kwargs 내부에 존재하는 진 템플릿 변수 이용
    def python_function2(**kwargs): 
        print(kwargs)
        print('ds:'+kwargs['ds'])
        print('ts:'+kwargs['ts'])
        print('data_interval_start:'+kwargs['data_interval_start'])
        print('data_interval_end:'+kwargs['data_interval_end'])
        print('task_instance:'+kwargs['task_instance'])

    python_t1 >> python_function2() # task decorator를 이용했을 때, 함수를 실행하기만 해도 task 생성 + 연결 가능능