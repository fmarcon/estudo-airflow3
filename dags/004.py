
from airflow.sdk import dag, task
import pendulum

@dag(
    dag_id="dag_004",
    start_date=pendulum.datetime(2025, 10, 23, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["2025"],
    params={"cadastro": 0}
)
def dag_004():
    """
    Esta é uma DAG de exemplo que demonstra o uso da API TaskFlow no Airflow 3.1.
    """
    @task
    def integrar_pessoa(**context):
        """Integra uma pessoa com base no cadastro fornecido."""
        cadastro = context['params'].get('cadastro', None)
        return {"nome": "Maria", "idade": 30, "cadastro": cadastro}
    dados_integrados = integrar_pessoa()

dag_004()