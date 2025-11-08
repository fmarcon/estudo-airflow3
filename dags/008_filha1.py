from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.task.trigger_rule import TriggerRule


@dag(
    dag_id="dag_008_filha1",
    start_date=None,
    catchup=False,
    tags=["2025"],
)
def criar_usuarios_pendentes():
    """
    DAG pai para criar usuários pendentes.
    """

    @task
    def iniciar_processo():
        print("Iniciando o processo de criação de usuários pendentes.")

    chamar_dag_filha = TriggerDagRunOperator(
        task_id="chamar_dag_filha",
        trigger_dag_id="dag_008_filha2",
        conf={"acao": "criar_usuarios_pendentes"},
        wait_for_completion=True,
        poke_interval=5,
        reset_dag_run=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,  # só dispara se tudo anterior deu certo
    )


    # Instancia tasks
    inicio = iniciar_processo()
    fim = chamar_dag_filha

    inicio >> fim

criarusuarios_pendentes_dag = criar_usuarios_pendentes()