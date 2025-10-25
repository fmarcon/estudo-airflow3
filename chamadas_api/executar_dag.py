from datetime import date, datetime
import requests
import json

TOKEN_URL = "http://localhost:8080/auth/token"  # ou o endpoint que você usa
AIRFLOW_API_BASE = "http://localhost:8080/api/v2"
DAG_ID = "dag_001"  # Substitua pelo ID da sua DAG

def get_token():
    response = requests.post(
        TOKEN_URL,
        json={"username": "airflow", "password": "airflow"},
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
    return response.json()["access_token"]

def trigger_dag(config=None):
    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    url = f"{AIRFLOW_API_BASE}/dags/{DAG_ID}/dagRuns"
    # Usa data e hora atual em UTC com precisão de segundos, formato ISO (ex: 2025-10-25T12:37:09Z)
    logical_date = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"Logical Date: {logical_date}")
    if config is not None:
        data = json.dumps(config)
    data = json.dumps({"logical_date": logical_date})
    response = requests.post(url, headers=headers, data=data)
    print(f"Status Code: {response.status_code}")
    #print(f"Response: {response.json()}")

# Exemplo de uso:
config = {"conf": {"logical_date": "2025-10-25T12:37:09.000Z"}}
trigger_dag(config)
