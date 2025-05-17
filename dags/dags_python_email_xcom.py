from airflow import DAG
from airflow.operators.email import EmailOperator
import pendulum
from airflow.decorators import task

with DAG(
    dag_id="dags_python_email_xcom",
    schedule="30 0 * * *",
    start_date=pendulum.datetime(2025, 5, 10, tz="Asia/Seoul"),
    catchup=False
) as dag:
    @task(task_id='something_task')
    def some_logic():
        from random import choice
        return choice(['success', 'failure'])

    email_pull = EmailOperator(
        task_id='send_email',
        to='3634lelee@naver.com',
        subject='{{ (data_interval_end.in_timezone("Asia/Seoul")) |ds }} some_logic 처리 결과',
        html_content='{{ (data_interval_end.in_timezone("Asia/Seoul")) |ds }} 처리 결과는 <br> \
                      {{ti.xcom_pull(task_ids="something_task")}} 입니다. <br>'
    )
    some_logic() >> email_pull 