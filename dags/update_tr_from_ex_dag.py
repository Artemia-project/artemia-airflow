# dags/update_tr_from_ex_dag.py
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.http.operators.http import HttpOperator
# GithubActionsRunStateSensor를 import 합니다.
from airflow.providers.github.sensors.github import GithubActionsRunStateSensor

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

with DAG(
    dag_id='update_tourism_from_exhibition_dag',
    default_args=default_args,
    description='Triggers a GitHub Action, waits for completion, and then updates tourism data.',
    # KST 자정(00:00)은 UTC 기준 15:00 입니다. (KST = UTC+9)
    schedule='0 15 * * *',
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['data-pipeline', 'github-action', 'crawling'],
) as dag:
    
    # 1. GitHub Actions 워크플로우를 트리거하는 Task (기존과 동일)
    trigger_github_action = HttpOperator(
        task_id='trigger_github_action_scraper',
        http_conn_id='github_api_conn',
        method='POST',
        endpoint='/repos/artemia-project/artemia-data-pipeline-githubaction/dispatches',
        headers={
            "Accept": "application/vnd.github.v3+json",
            "Authorization": "token {{ conn.github_api_conn.password }}",
            "Content-Type": "application/json",
        },
        data='{"event_type": "run-scraper-trigger"}',
        log_response=True,
    )

    # 2. 트리거된 워크플로우의 완료를 기다리는 Sensor Task (새로 추가)
    wait_for_github_action = GithubActionsRunStateSensor(
        task_id='wait_for_github_action_completion',
        github_conn_id='github_api_conn',  # 트리거에 사용한 것과 동일한 Connection ID
        owner='artemia-project',           # GitHub 레포지토리 소유자
        repo='artemia-data-pipeline-githubaction', # GitHub 레포지토리 이름
        # `repository_dispatch`로 트리거된 워크플로우의 가장 최근 실행을 감지합니다.
        # 이 옵션은 Airflow 2.4.0+ 및 최신 github provider에서 사용 가능합니다.
        # 만약 이 옵션이 없다면, commit_sha나 run_id를 특정해야 해서 복잡해집니다.
        # `external_id`는 repository_dispatch의 client_payload를 참조할 때 사용될 수 있습니다.
        # 여기서는 가장 최근 실행을 감지하는 것으로 충분합니다.
        workflow_file_name='run_scraper.yml', # 감시할 워크플로우 파일 이름
        # `poke_interval`은 상태를 확인하는 주기(초)입니다. API Rate Limit을 고려하여 너무 짧지 않게 설정합니다.
        poke_interval=60, # 60초마다 확인
        # `timeout`은 Sensor가 최대로 기다리는 시간(초)입니다. 워크플로우 최대 실행 시간보다 길게 설정합니다.
        timeout=3600, # 1시간
    )
    
    # 성공 알림 Task
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

    # 실패 알림 Task
    notify_failure = HttpOperator(
        task_id='notify_failure',
        method='POST',
        http_conn_id='powerautomate_webhook',
        endpoint='',
        data='{"message":"Failed! Data pipeline [update_tourism_from_exhibition] failed."}',
        headers={"Content-Type": "application/json"},
        trigger_rule='one_failed',
        log_response=True,
    )

    # Task 의존성 재설정
    # 트리거 -> 대기(Sensor) -> 결과에 따른 알림
    trigger_github_action >> wait_for_github_action >> [notify_success, notify_failure]
