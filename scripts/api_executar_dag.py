from datetime import UTC, datetime
from pprint import pprint

import requests

USERNAME = "airflow"
PASSWORD = "airflow"
HOST = "localhost"
PORT = 8080
BASE_URL = f"http://{HOST}:{PORT}/api/v2"
TOKEN_URL = f"http://{HOST}:{PORT}/auth/token"


def get_token(session: requests.Session, token_url: str, username: str, password: str) -> str:
    """
    Obtem um access token do endpoint de autenticação.
    """
    resp = session.post(
        token_url,
        json={"username": username, "password": password},
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json().get("access_token", "")


def verificar_status_dag_run(session: requests.Session, base_url: str, dag_id: str, token: str,) -> str:
    """
    Verifica o status do dag_run especificado.
    """
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = f"{base_url}/dags/{dag_id}"
    resp = session.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    pprint(f"Resposta da verificação do status da DAG: {resp.json()}")
    return resp.json().get("is_paused", "unknown")


def executar_dag(session: requests.Session, base_url: str, dag_id: str, token: str) -> str:
    """
    Dispara a DAG e retorna o dag_run_id criado.
    """
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    logical_date = datetime.now(UTC).replace(microsecond=0).isoformat()
    payload = {"logical_date": logical_date}
    pprint(f"Payload para disparo da DAG: {payload}")

    url = f"{base_url}/dags/{dag_id}/dagRuns"
    resp = session.post(url, headers=headers, json=payload, timeout=10)
    resp.raise_for_status()
    pprint(f"Resposta do disparo da DAG: {resp.json()}")
    return resp.json().get("dag_run_id")


if __name__ == "__main__":
    # Configurações iniciais
    dag_id = "dag_002"

    # Cria sessão HTTP
    session = requests.Session()
    
    # Obtém token
    token = get_token(session=session, token_url=TOKEN_URL, username=USERNAME, password=PASSWORD)
    
    # Executa dag e obtém o dag_run_id
    dag_run_id = executar_dag(session=session, base_url=BASE_URL, dag_id="dag_002", token=token)
    pprint(f"DAG disparada com dag_run_id: {dag_run_id}")

    # Verifica o status de uma dag
    status = verificar_status_dag_run(
        session=session,
        base_url=BASE_URL,
        dag_id=dag_id,
        token=token,
    )
    pprint(f"Status final do dag_run {dag_id}: {status}")
    