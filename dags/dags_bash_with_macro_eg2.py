from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eg2",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025, 5, 10, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    # start_date = 2주전 월요일, end_date= 2주전 토요일
    bash_task_2 = BashOperator(
        task_id='bash_task_2', # datetime.utild은 datetime을 템플릿 변수 내에서 사용할 수 있도록 해주는 모듈
        env={'START_DATE':'{{ (data_interval_end.in_timezone("Asia/Seoul")-macros.dateutil.relativedelta.relativedelta(days=19))|ds}}', 
             'END_DATE': '{{ (data_interval_end.in_timezone("Asia/Seoul")-macros.dateutil.relativedelta.relativedelta(days=19))|ds}}'
             },
        bash_command='echo "Start date: $START_DATE" && echo "End date: $$END_DATE"'
    )