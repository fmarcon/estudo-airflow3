from datetime import datetime

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator


# 🧠 Simula uma função que verifica se há pessoas novas
def get_new_people():
    # Exemplo real: consultar banco, API, etc.
    # Aqui vamos simular que às vezes há pessoas novas, às vezes não.
    from random import choice
    #return ["Ana", "Carlos"] if choice([True, False]) else []
    return []  # Simulando que não há pessoas novas

@dag(
    dag_id="dag_005",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["2025"],
)
def integracao_pessoas():
    """
    DAG de integração de pessoas.
    Se não houver pessoas novas para integrar, a DAG termina com tasks 'skipped'.
    """

    @task
    def checar_pessoas_novas():
        pessoas = get_new_people()
        if not pessoas:
            print("✅ Nenhuma pessoa nova para integrar. Encerrando DAG.")
            return None
        print(f"🔍 Pessoas novas encontradas: {pessoas}")
        return pessoas

    @task.branch
    def decidir_fluxo(pessoas):
        """Decide se continua a integração ou encerra."""
        if not pessoas:
            print("xxxxxxx")
            return "sem_novos"
        print("yyyyyy")
        return "integrar_pessoas"

    @task
    def integrar_pessoas(pessoas):
        for p in pessoas:
            print(f"Integrando pessoa: {p}")
        print("✅ Integração concluída.")

    sem_novos = EmptyOperator(task_id="sem_novos")

    fim = EmptyOperator(
        task_id="fim",
        #trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    pessoas = checar_pessoas_novas()
    next_step = decidir_fluxo(pessoas)
    integrar_pessoas(pessoas) >> fim
    sem_novos >> fim

integracao_pessoas()
