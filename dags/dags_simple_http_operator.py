
from airflow import DAG
from airflow.decorators import task

import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator


with DAG(
    dag_id='dags_simple_http_operator',
    start_date=pendulum.datetime(2025, 5, 10, tz='Asia/Seoul'),
    catchup=False,
    schedule=None
) as dag:

    '''서울시 공공자전거 대여소 정보'''
    tb_cycle_station_info = SimpleHttpOperator(
        task_id='tb_cycle_station_info',
        http_conn_id='openapi.seoul.go.kr',
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/tbCycleStationInfo/1/10/', # openapi 값을 variable => 노출 X, 다른 DAG에서 바뀌어도 전역변수 값바꿔서 일괄적용 가능능
        method='GET',
        headers={'Content-Type': 'application/json',
                        'charset': 'utf-8',
                        'Accept': '*/*'
                        }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='tb_cycle_station_info') #XCOM값 가져오기
        import json
        from pprint import pprint

        pprint(json.loads(rslt)) # json 형식으로 출력, pprint로 보기 좋게 출력
        
    tb_cycle_station_info >> python_2()