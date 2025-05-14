from airflow.models.dag import DAG
import datetime
import pendulum # Datetime 타입을 쉽쉽게 사용할 수 있게
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_operator", # DAG 이름 (Python 파일명과는 상관 없음, 일치시키는 게 좋음.)
    schedule="0 0 * * *", # 5개 항목: 분,시, 일,월,요일 => 언제 작동할지 설정정
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"), # DAG 시작 날짜
    catchup=False, # False: DAG이 시작된 시점부터 스케줄에 맞춰서 실행, True: DAG이 시작된 시점부터 이전까지의 모든 스케줄을 실행 == 누락 시간 반영 여부
    # dagrun_timeout=datetime.timedelta(minutes=60), # DAG이 실행되는 최대 시간
    # tags=["example", "example2"], # DAG 태그
    # params={"example_key": "example_value"}, # Task에 전달할 공통 파라미터
) as dag:
    bash_t1 = BashOperator( # Operator가 task를 생성
        task_id="bash_t1", # Task ID, graph 상에서 보여지는 이름
        bash_command="echo whoami", # 어떤 명령어를 실행할지, echo는 출력하는 명령어
    )
    bash_t2 = BashOperator( # Operator가 task를 생성
        task_id="bash_t2", # Task ID, graph 상에서 보여지는 이름
        bash_command="echo $HOSTNAME", # 어떤 명령어를 실행할지, echo는 출력하는 명령어
    )
    bash_t1 >> bash_t2 # Task 간의 순서 설정, bash_t1이 끝나야 bash_t2가 실행됨
    