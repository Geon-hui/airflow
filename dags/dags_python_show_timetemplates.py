from airflow import DAG
from airflow.decorators import task
import pendulum
from pprint import pprint

with DAG(
    dag_id="dags_python_show_timetemplates",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025, 5, 10, tz="Asia/Seoul"),
    catchup=True,
    tags=["test"],
) as dag:

    @task(task_id='python_task')
    def show_templates(**kwargs):
        pprint(kwargs)

    show_templates() 