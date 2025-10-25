from datetime import datetime, UTC
import time
import token
import requests
import json

TOKEN_URL = "http://localhost:8080/auth/token"
AIRFLOW_API_BASE = "http://localhost:8080/api/v2"
DAG_ID = "dag_001" 

def get_token():
    response = requests.post(
        TOKEN_URL,
        json={"username": "airflow", "password": "airflow"},
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
    return response.json()["access_token"]

def trigger_dag(token, config=None):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    url = f"{AIRFLOW_API_BASE}/dags/{DAG_ID}/dagRuns"
    data = json.dumps({"logical_date": logical_date})
    response = requests.post(url, headers=headers, data=data)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    return response.json().get("dag_run_id")


def verificar_dag(token, dag_run_id):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    url = f"{AIRFLOW_API_BASE}/dags/{DAG_ID}/dagRuns/{dag_run_id}/wait"
    response = requests.get(url, headers=headers)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")


if __name__ == "__main__":
    token = get_token()
    logical_date = datetime.now(UTC).replace(microsecond=0).isoformat()
    config = {"conf": {"logical_date": logical_date}}
    dag_run_id = trigger_dag(token, config)
    time.sleep(5)  # Aguarda alguns segundos antes de verificar
    verificar_dag(token, dag_run_id)
