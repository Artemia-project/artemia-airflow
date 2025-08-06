# dags/update_tr_from_ex_dag.py
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.http.operators.http import HttpOperator

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

with DAG(
    dag_id='update_tourism_from_exhibition_dag',
    default_args=default_args,
    description='Triggers a GitHub Action to crawl and update tourism data.',
    # KST 자정(00:00)은 UTC 기준 15:00 입니다. (KST = UTC+9)
    schedule='0 15 * * *',
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['data-pipeline', 'github-action', 'crawling'],
) as dag:
    
    # GitHub Actions 워크플로우를 트리거하는 Task
    trigger_github_action = HttpOperator(
        task_id='trigger_github_action_scraper',
        http_conn_id='github_api_conn',  # Airflow에 설정해야 할 Connection ID
        method='POST',
        # {owner}/{repo}를 실제 정보로 수정합니다.
        endpoint='/repos/artemia-project/artemia-data-pipeline-githubaction/dispatches',
        headers={
            "Accept": "application/vnd.github.v3+json",
            # Airflow Connection의 Password 필드에서 PAT를 가져옵니다.
            "Authorization": "token {{ conn.github_api_conn.password }}",
            "Content-Type": "application/json",
        },
        # repository_dispatch의 event_type을 지정합니다. (워크플로우 파일의 `types`와 일치해야 함)
        data='{"event_type": "run-scraper-trigger"}',
        log_response=True,
    )

    # 성공 알림 Task (기존 코드 활용)
    notify_success = HttpOperator(
        task_id='notify_success',
        method='POST',
        http_conn_id='powerautomate_webhook',
        endpoint='',
        data='{"message":"Success! Data pipeline [update_tourism_from_exhibition] completed."}',
        headers={"Content-Type": "application/json"},
        trigger_rule='all_success',
        log_response=True,
    )

    # 실패 알림 Task (기존 코드 활용)
    notify_failure = HttpOperator(
        task_id='notify_failure',
        method='POST',
        http_conn_id='powerautomate_webhook',
        endpoint='',
        data='{"message":"Failed! Data pipeline [update_tourism_from_exhibition] failed."}',
        headers={"Content-Type": "application/json"},
        trigger_rule='one_failed', # 상위 Task가 하나이므로 one_failed가 더 적합합니다.
        log_response=True,
    )

    # Task 의존성 설정
    trigger_github_action >> [notify_success, notify_failure]
