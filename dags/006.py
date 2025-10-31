from datetime import datetime
from random import choice

from airflow.operators.empty import EmptyOperator
from airflow.sdk import dag, task
from airflow.utils.trigger_rule import TriggerRule


@dag(
    dag_id="dag_006",
    start_date=datetime(2024, 1, 1),
    schedule="* * * * *",
    catchup=False,
    tags=["2025"],
)
def execucao_dag():
    """
    """
    @task.branch
    def decidir_fluxo():
        if choice([True, False]):
            return "fluxo2"
        return "fluxo1"

    @task
    def fluxo1():
        print("Fluxo 1 executado.")

    @task
    def fluxo2():
        print("Fluxo 2 executado.")

    fim = EmptyOperator(
        task_id="fim",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    
    fluxo_escolhido = decidir_fluxo()
    f1 = fluxo1()
    f2 = fluxo2()

    # Agora o branch controla qual fluxo segue:
    fluxo_escolhido >> [f1, f2] >> fim

execucao_dag()
