from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='call_azure_function_dag',
    default_args=default_args,
    description='DAG to call Azure Function',
    schedule='@daily',  # ← 여기 수정
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=['azure', 'http'],
) as dag:

    call_azure_function = HttpOperator(
        task_id='call_azure_function',
        method='GET',
        http_conn_id='azure_function_conn',
        endpoint='api/hello1?',
        headers={"Content-Type": "application/json"},
        log_response=True,
    )

    call_azure_function
