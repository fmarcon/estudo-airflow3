from airflow.decorators import dag, task


@dag(
    dag_id="dag_008_filha2",
    start_date=None,
    catchup=False,
    tags=["2025"],
)
def criar_pessoas_usuarios_pendentes():
    """
    DAG pai para criar usuários pendentes.
    """

    @task
    def iniciar_processo():
        print("Iniciando o processo de criação de usuários pendentes.")

    @task
    def finalizar_processo():
        print("Finalizando o processo de criação de usuários pendentes.")

    # Instancia tasks
    inicio = iniciar_processo()
    fim = finalizar_processo()

    inicio >> fim

criar_pessoas_usuarios_pendentes_dag = criar_pessoas_usuarios_pendentes()
