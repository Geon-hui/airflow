import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator

with DAG(
    dag_id="dags_branch_python_operator",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2025, 5, 10, tz="Asia/Seoul"),
    catchup=False
) as dag:

    def select_randome():
        import random

        item_list=['A','B','C']
        selected_item = random.choice(item_list)

        if selected_item == 'A':
            reruen 'task_a'
        if selected_item in ['B','C']:
            return ['task_b','task_c']:
    
    python_branch_task = BranchPythonOperator(
        task_id='python_branch_task',
        python_callable=common_func
    )

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func(),
        op_kwargs={'selected':'A'}
    )
    task_b =PythonOperator(
        task_id='task_b',
        python_callable=common_func(),
        op_kwargs={'selected':'B'}
    )
    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func(),
        op_kwargs={'selected':'C'}
    )

    python_branch_task >> [task_a, task_b, task_c]