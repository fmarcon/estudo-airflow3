from airflow.sdk import DAG, Param, get_current_context, task

with DAG(
    "dag_010",
    params={
        "x": Param(5, type="integer", minimum=3),   # Valor padrão e validação de tipo
        "texto": Param("abc", type="string", maxLength=10),
    },
    tags=["2025"],
) as dag:

    @task
    def mostra_parametros():
        contexto = get_current_context()            # Captura contexto da DAG
        print(f"x: {contexto['params']['x']}")     # Acesso ao parâmetro x
        print(f"texto: {contexto['params']['texto']}") # Acesso ao parâmetro texto

    @task
    def mostra_parametros1():
        contexto = get_current_context()            # Captura contexto da DAG
        for key, value in contexto.items():
            print(f"{key}: {value}")
            print("-----")
        #print("Contexto da DAG no segundo passo:", contexto)
        print(f"x: {contexto['params']['x']}")     # Acesso ao parâmetro x
        print(f"texto: {contexto['params']['texto']}") # Acesso ao parâmetro texto

    passo001 = mostra_parametros()
    passo002 = mostra_parametros1()

    passo001 >> passo002

