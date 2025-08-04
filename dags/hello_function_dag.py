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
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    description='DAG to call Azure Function',
    schedule='@daily',
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
        
        retries=3,
        retry_delay=timedelta(minutes=3),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=10),
    )

    # Success notification task
    notify_success = HttpOperator(
        task_id='notify_success',
        method='POST',
        http_conn_id='powerautomate_webhook',
        endpoint='',
        data='{"message":"Success! Azure Function API Call"}',
        trigger_rule='all_success',
        headers={"Content-Type": "application/json"},
        log_response=True,
    )

    # Failure notification task
    notify_failure = HttpOperator(
        task_id='notify_failure',
        method='POST',
        http_conn_id='powerautomate_webhook',
        endpoint='',
        data='{"message":"Failed! Azure Function API Call"}',
        trigger_rule='all_failed',
        headers={"Content-Type": "application/json"},
        log_response=True,
    )

    # Set up task dependencies
    call_azure_function >> [notify_success, notify_failure]
