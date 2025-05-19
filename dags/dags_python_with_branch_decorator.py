from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(
    dag_id="dags_base_branch_operator",
    start_date=datetime(2025, 5, 10),
    schedule="None",
    catchup=False
) as dag:

    @task.branch(task_id='python_branch_task')
    def select_random():
        import random
        item_lst=['A','B','C']
        select_item=random.choice(item_lst)
        if select_item == 'A':
            return 'task_a'
        elif select_item == 'B':
            return 'task_b'
        else:
            return 'task_c'

    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )
    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )
    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )
    select_random() >> [task_a, task_b, task_c]

    