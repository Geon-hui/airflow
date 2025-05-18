import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_variable",
    schedule="10 9 * * *",
    start_date=pendulum.datetime(2025, 5, 10, tz="Asia/Seoul"),
    catchup=False
) as dag:

    va_value=Variable.get('sample_key') # 파이썬 문법으로 직접 variable 값 가져오기기

    bash_var_1=BashOperator(
        task_id='bash_var_1',
        bash_command=f"echo variable :{var_value}"
    )
    bash_var_2 = BashOperator( 
        task_id='bash_var_2',
        bash_command="echo variable:{{var.value.sample_key}}" # 템플릿 변수로 직접 variable 값 가져오기 - 권고 
    )