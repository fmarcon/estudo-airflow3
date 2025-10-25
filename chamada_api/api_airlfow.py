import requests
from datetime import datetime, UTC
import time


BASE_URL = "http://localhost:8080"
URL_API = "/api/v2"
BASE_URL_TOKEN = f"{BASE_URL}/auth/token"


class APIAirflow:
    def __init__(self, dag_id, username, password):
        self.base_url = f"{BASE_URL}/{URL_API}"
        self.base_url_token = BASE_URL_TOKEN
        self.dag_id = dag_id
        self.username = username
        self.password = password
        self.token = self._get_token()
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

    def _get_token(self):
        response = requests.post(
            BASE_URL_TOKEN,
            json={"username": self.username, "password": self.password},
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        self.token = response.json()["access_token"]
        return self.token

    def executar_dag(self, cadastro):
        logical_date = datetime.now(UTC).replace(microsecond=0).isoformat()
        config = {"logical_date": logical_date, "conf": {"cadastro": cadastro}}
        url = f"{self.base_url}/dags/{self.dag_id}/dagRuns"
        response = requests.post(url, headers=self.headers, json=config)
        response.raise_for_status()
        return response.json().get("dag_run_id")
    
    def verificar_dag(self, dag_run_id):
        if self.token is None:
            self.get_token()
        url = f"{self.base_url}/dags/{self.dag_id}/dagRuns/{dag_run_id}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json() 

    def imprimir_retorno(self, retorno):
        for chave, valor in retorno.items():
            if valor:
                print(f"{chave}: {valor}")


if __name__ == "__main__":

    dag_id = "dag_004"
    username = "airflow"
    password = "airflow"
    
    api = APIAirflow(dag_id, username, password)
    dag_run_id = api.executar_dag(cadastro=12345)
    print(f"DAG Run ID: {dag_run_id}")
    time.sleep(5) 
    
    retorno = api.verificar_dag(dag_run_id)
    dag_status = retorno.get("state")
    print(f"Status dag: {dag_status}")

