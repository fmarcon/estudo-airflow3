# dag_exemplo_3_1.py
import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="dag_001",
    start_date=pendulum.datetime(2025, 10, 23, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["2025"]
)
def exemplo_dag_taskflow():
    """
    Esta é uma DAG de exemplo que demonstra o uso da API TaskFlow no Airflow 3.1.
    """

    @task
    def extrair_dados():
        """Extrai alguns dados de exemplo."""
        print("Executando a tarefa de extração de dados...")
        return {"nome": "Maria", "idade": 30}

    @task
    def transformar_dados(dados_entrada):
        """Transforma os dados extraídos."""
        print(f"Executando a tarefa de transformação com dados: {dados_entrada}")
        dados_transformados = dados_entrada.copy()
        dados_transformados["status"] = "processado"
        return dados_transformados

    @task
    def carregar_dados(dados_saida):
        """Carrega os dados transformados."""
        print(f"Executando a tarefa de carregamento com dados: {dados_saida}")
        print("Dados carregados com sucesso!")

    # Definindo o fluxo de tarefas
    dados_extraidos = extrair_dados()
    dados_transformados = transformar_dados(dados_extraidos)
    carregar_dados(dados_transformados)

# Instanciando a DAG para que o Airflow possa descobri-la
exemplo_dag_taskflow()
