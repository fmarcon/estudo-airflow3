from typing import List

from airflow.sdk import dag, task, task_group


@dag(
    dag_id="dag_009",
    schedule=None,
    catchup=False,

    # Executa somente uma instância da dag de cada vez.
    max_active_runs=1,
    
    # Limita o número total de tarefas ativas desta DAG, mesmo que o pool permita mais.
    max_active_tasks=10,
    
    tags=["2025"],
)
def etl_clientes_dag():

    @task
    def buscar_clientes() -> List[str]:
        # Simulação de consulta Oracle
        return [f"cliente_{i}" for i in range(1, 100 + 1)] # Simula 60 clientes

    @task_group(group_id="taskgroup_por_cliente")
    def taskgroup_por_cliente(cliente: str):

        @task(pool="limited_pool")
        def extrair(cliente: str):
            print(f"[{cliente}] Extraindo dados...")
            return f"dados_brutos_{cliente}"

        @task(pool="limited_pool")
        def transformar(dados_brutos: str):
            print(f"[{dados_brutos}] Transformando dados...")
            return f"dados_transformados_{dados_brutos}"

        @task(pool="limited_pool")
        def carregar(dados_transformados: str):
            print(f"[{dados_transformados}] Carregando no destino final!")

        # Encadeia as subtarefas dentro do grupo
        dados = extrair(cliente)
        dados_t = transformar(dados)
        carregar(dados_t)

    @task
    def finalizar():
        print("Todos os pipelines concluídos com sucesso!")

    clientes = buscar_clientes()

    # Cada cliente cria dinamicamente um grupo
    taskgroup = taskgroup_por_cliente.expand(cliente=clientes)

    taskgroup >> finalizar()


# Instancia a DAG
etl_clientes_dag()
