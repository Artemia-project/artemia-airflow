# dags/update_tr_from_ex_dag.py
from __future__ import annotations

import pendulum
import json

from airflow.models.dag import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.sensors.external_task import ExternalTaskSensor

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    # 재시도는 GitHub Actions에서 관리하므로 Airflow에서는 끔
    'retries': 0,
}

# GitHub 및 Airflow 관련 상수
GITHUB_REPO = "artemia-project/artemia-data-pipeline-githubaction"
GITHUB_CONN_ID = "github_api_conn" # Airflow UI에 설정된 GitHub Connection ID
POWERAUTOMATE_CONN_ID = "powerautomate_webhook" # Power Automate Webhook Connection ID

with DAG(
    dag_id='update_tourism_from_exhibition_dag',
    default_args=default_args,
    description='Triggers a GitHub Action and waits for its completion callback.',
    # KST 자정(00:00)은 UTC 기준 15:00 입니다.
    schedule='0 15 * * *',
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['data-pipeline', 'github-action', 'webhook'],
    # 워크플로우가 2시간 이상 걸리면 타임아웃 처리
    dagrun_timeout=pendulum.duration(hours=2),
) as dag:

    # Task 1: GitHub Actions 워크플로우 트리거
    trigger_github_action = HttpOperator(
        task_id='trigger_github_action_scraper',
        http_conn_id=GITHUB_CONN_ID,
        method='POST',
        endpoint=f'/repos/{GITHUB_REPO}/dispatches',
        headers={
            "Accept": "application/vnd.github.v3+json",
            "Authorization": "token {{ conn.github_api_conn.password }}",
            "Content-Type": "application/json",
        },
        # GitHub Actions에 콜백에 필요한 정보를 client_payload로 전달
        data=json.dumps({
            "event_type": "run-scraper-trigger",
            "client_payload": {
                # Jinja 템플릿을 사용해 현재 DAG와 실행 정보를 동적으로 전달
                "dag_id": "{{ dag.dag_id }}",
                "airflow_run_id": "{{ run_id }}"
            }
        }),
        log_response=True,
    )

    # Task 2: GitHub Actions로부터의 콜백을 기다리는 센서
    # 이 Task는 GitHub Actions가 자신의 상태를 'success' 또는 'failed'로
    # 업데이트 해줄 때까지 대기합니다.
    wait_for_github_callback = ExternalTaskSensor(
        task_id='wait_for_github_callback',
        # 이 센서 자체의 상태가 외부(GitHub Actions)에 의해 변경되기를 기다림
        external_dag_id=dag.dag_id,
        # 특정 Task를 기다리는 것이 아니라, 이 센서 자체가 외부에서 완료되기를 기다림
        # 이를 위해선 GitHub Actions가 이 Task의 상태를 직접 업데이트해야 함
        external_task_id='wait_for_github_callback', # 자기 자신의 Task ID를 명시
        mode='poke',
        poke_interval=60, # 60초마다 자신의 상태를 체크
        timeout=7100, # 2시간 미만으로 타임아웃 설정 (1시간 58분 20초)
        allowed_states=['success'], # 'success' 상태가 되면 통과
        failed_states=['failed', 'skipped'], # 'failed' 또는 'skipped' 상태가 되면 실패
    )

    # Task 3: 성공 알림 Task
    notify_success = HttpOperator(
        task_id='notify_success',
        http_conn_id=POWERAUTOMATE_CONN_ID,
        method='POST',
        endpoint='', # Power Automate Webhook URL의 Host 이후 부분을 입력
        data='{"message":"Success! Data pipeline [update_tourism_from_exhibition] completed."}',
        headers={"Content-Type": "application/json"},
        # trigger_rule은 기본값 'all_success'를 사용합니다.
        # 즉, 상위 Task(wait_for_github_callback)가 성공해야만 실행됩니다.
        log_response=True,
    )

    # Task 4: 실패 알림 Task
    notify_failure = HttpOperator(
        task_id='notify_failure',
        http_conn_id=POWERAUTOMATE_CONN_ID,
        method='POST',
        endpoint='', # Power Automate Webhook URL의 Host 이후 부분을 입력
        data='{"message":"Failed! Data pipeline [update_tourism_from_exhibition] failed."}',
        headers={"Content-Type": "application/json"},
        # trigger_rule을 'one_failed'로 설정하여, 업스트림 Task 중 하나라도 실패하면 실행되도록 함
        trigger_rule='one_failed',
        log_response=True,
    )

    # Task 의존성 설정
    # 1. GitHub Actions를 트리거한다.
    trigger_github_action >> wait_for_github_callback

    # 2. 콜백 센서의 성공 여부에 따라 성공 알림을 보낸다.
    wait_for_github_callback >> notify_success
    
    # 3. 실패 알림은 트리거 또는 센서가 실패했을 때 실행되도록 연결한다.
    #    (trigger_rule='one_failed'가 이 역할을 자동으로 수행하므로,
    #    성공 경로와 실패 경로를 모두 연결해줍니다.)
    [trigger_github_action, wait_for_github_callback] >> notify_failure
