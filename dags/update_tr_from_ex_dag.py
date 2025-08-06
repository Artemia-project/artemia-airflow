from __future__ import annotations

import pendulum
import time
import requests
from airflow.exceptions import AirflowException

from airflow.models.dag import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection

# =============================================================================
# Helper Functions
# =============================================================================

def check_github_action_run_status(response: requests.Response) -> bool:
    """
    HttpSensor에서 사용할 응답 확인 함수. GitHub Action 실행 결과를 확인합니다.
    - 성공적으로 완료되면 True를 반환합니다.
    - 실패하면 AirflowException을 발생시켜 Sensor를 실패시킵니다.
    - 아직 실행 중이면 False를 반환하여 Sensor가 계속 대기하도록 합니다.
    """
    if response.status_code == 200:
        run_data = response.json()
        status = run_data.get("status")
        conclusion = run_data.get("conclusion")

        if status == "completed":
            if conclusion == "success":
                print(f"GitHub Action run succeeded. Status: {status}, Conclusion: {conclusion}")
                return True
            else:
                # 워크플로우가 실패, 취소 등 다른 상태로 완료된 경우
                error_message = f"GitHub Action run did not succeed. Final status: {status}, Conclusion: {conclusion}"
                print(error_message)
                raise AirflowException(error_message)
        else:
            # 아직 'in_progress' 또는 'queued' 상태
            print(f"GitHub Action is still running. Current status: {status}")
            return False
    else:
        # API 호출 자체가 실패한 경우 (404 Not Found, 401 Unauthorized 등)
        error_message = f"Failed to check GitHub Action status. HTTP Status: {response.status_code}, Response: {response.text}"
        print(error_message)
        # 이 경우, Sensor가 계속 시도하는 대신 실패하도록 예외를 발생시키는 것이 좋습니다.
        raise AirflowException(error_message)


def get_latest_workflow_run_id(owner: str, repo: str, workflow_file_name: str, github_conn_id: str) -> int:
    """
    PythonOperator에서 사용할 함수.
    repository_dispatch로 트리거된 특정 워크플로우의 가장 최근 run_id를 찾아서 반환합니다.
    GitHub API에 run이 등록될 때까지 잠시 기다린 후 조회합니다.
    """
    # 트리거 API 호출 후 GitHub에서 run이 생성될 시간을 벌기 위해 잠시 대기합니다.
    print("Waiting for 15 seconds for the workflow run to be created on GitHub...")
    time.sleep(15)

    conn = Connection.get_connection_from_secrets(github_conn_id)
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"token {conn.password}",
    }
    api_url = f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/{workflow_file_name}/runs"
    params = {"event": "repository_dispatch", "per_page": 1}
    
    print(f"Fetching latest run from: {api_url}")
    response = requests.get(api_url, headers=headers, params=params)
    response.raise_for_status()
    
    runs = response.json().get("workflow_runs")
    if not runs:
        raise AirflowException("Could not find any recent workflow runs triggered by repository_dispatch. Check if the workflow was triggered correctly.")
        
    latest_run_id = runs[0]['id']
    print(f"Found latest workflow run ID: {latest_run_id}")
    return latest_run_id


# =============================================================================
# DAG Definition
# =============================================================================

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

with DAG(
    dag_id='update_tourism_from_exhibition_dag',
    default_args=default_args,
    description='Triggers a GitHub Action, waits for completion, and then notifies the result.',
    schedule='0 15 * * *', # KST 자정
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['data-pipeline', 'github-action', 'http-sensor'],
    # DAG가 사용하는 Connection 정보를 명시적으로 표시 (가독성/관리 용이성)
    doc_md="""
    ### GitHub Action Trigger and Monitor DAG
    This DAG performs the following steps:
    1. **trigger_github_action_scraper**: Triggers a GitHub Actions workflow using a `repository_dispatch` event.
    2. **get_latest_run_id**: Fetches the `run_id` of the most recently triggered workflow.
    3. **wait_for_github_action_completion**: Uses an `HttpSensor` to poll the status of the workflow run until it is complete.
    4. **Notification**: Sends a success or failure notification based on the final status of the DAG.
    
    **Required Connections:**
    - `github_api_conn`: HTTP Connection to `https://api.github.com` with a Personal Access Token (PAT) in the password field.
    - `powerautomate_webhook`: HTTP Connection for sending notifications.
    """,
) as dag:
    
    GITHUB_OWNER = 'artemia-project'
    GITHUB_REPO = 'artemia-data-pipeline-githubaction'
    WORKFLOW_FILE_NAME = 'run_scraper.yml'
    GITHUB_CONN_ID = 'github_api_conn'

    # Task 1: GitHub Actions 워크플로우 트리거
    trigger_github_action = HttpOperator(
        task_id='trigger_github_action_scraper',
        http_conn_id=GITHUB_CONN_ID,
        method='POST',
        endpoint=f'/repos/{GITHUB_OWNER}/{GITHUB_REPO}/dispatches',
        headers={
            "Accept": "application/vnd.github.v3+json",
            "Authorization": "token {{ conn.github_api_conn.password }}",
            "Content-Type": "application/json",
        },
        data='{"event_type": "run-scraper-trigger"}',
        log_response=True,
    )

    # Task 2: 방금 트리거한 워크플로우의 run_id 가져오기
    get_run_id = PythonOperator(
        task_id='get_latest_run_id',
        python_callable=get_latest_workflow_run_id,
        op_kwargs={
            'owner': GITHUB_OWNER,
            'repo': GITHUB_REPO,
            'workflow_file_name': WORKFLOW_FILE_NAME,
            'github_conn_id': GITHUB_CONN_ID
        }
    )

    # Task 3: run_id를 사용하여 워크플로우 완료를 기다리는 Sensor
    wait_for_github_action = HttpSensor(
        task_id='wait_for_github_action_completion',
        http_conn_id=GITHUB_CONN_ID,
        endpoint=f"/repos/{GITHUB_OWNER}/{GITHUB_REPO}/actions/runs/{{{{ ti.xcom_pull(task_ids='get_latest_run_id') }}}}",
        # <<< --- 이 부분이 핵심 수정 사항입니다 --- >>>
        headers={
            "Accept": "application/vnd.github.v3+json",
            "Authorization": "token {{ conn.github_api_conn.password }}"
        },
        response_check=check_github_action_run_status,
        poke_interval=60,
        timeout=3600,
        mode='poke',
    )
    
    # Task 4: 성공 알림
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

    # Task 5: 실패 알림
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

    # Task 의존성 설정
    trigger_github_action >> get_run_id >> wait_for_github_action >> [notify_success, notify_failure]
