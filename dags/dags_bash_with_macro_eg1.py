from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = "dags_bash_with_macro_eg1",
    schedule="10 0 L * *",
    start_date=pendulum.datetime(2023, 8, 1, tz="UTC"),
    catchup=False
) as dag:
    
    # START_DATE : 전월 말일, END_DATE : 1일 전
    bash_task_t1 = BashOperator(
        task_id = "bash_task_t1",
        # UTC기준이 아닌 한국 시간기준이라면 data_interval_start.in_timezone("Asia/Seoul") 
        env={'START_DATE':'{{ data_interval_start.in_timezone("Asia/Seoul") | ds}}',
             'END_DATE':'{{ (data_interval_end.int_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds}}'
             },
             bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )
