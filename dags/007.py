from datetime import datetime

from airflow.decorators import dag, task
from click import confirm


@dag(
    dag_id="dag_007",
    start_date=None,
    schedule="* * * * *",
    catchup=False,
    tags=["2025"],
)
def execucao_dag():
    """
    """

    @task
    def inicio():
        print("Executando início.")

    @task
    def retorna_valor():
        return 11

    @task.branch
    def verifica_valor(valor):
        if valor == 1:
            return "valor_igual_1"
        return "valor_diferente_1"
    
    @task
    def valor_igual_1():
        print("Valor é igual a 1.")

    @task
    def valor_diferente_1():
        print("Valor é diferente de 1.")

    @task.branch
    def executar_outra_escolha(valor):
        if valor == 2:
            return "valor_igual_2"
        return "valor_diferente_de_2"

    @task
    def valor_igual_2():
        print("Valor é igual a 2.")
    
    @task
    def valor_diferente_de_2():
        print("Valor é diferente de 2.")


    @task(trigger_rule="one_success")
    def fim():
        print("Executando fim.")
    
    # Instancia tasks
    inicio = inicio()
    valor = retorna_valor()
    verifica = verifica_valor(valor)
    valor_igual = valor_igual_1()
    valor_diferente = valor_diferente_1()
    valor_igual_2 = valor_igual_2()
    valor_diferente_de_2 = valor_diferente_de_2()
    executar_outra_escolha = executar_outra_escolha(valor)
    fim= fim()

    # Conectar dependências de forma explícita e legível
    inicio >> valor >> verifica >> [valor_igual, valor_diferente]
    
    # Se valor igual, finaliza
    valor_igual >> fim

    # Se valor diferente, executa outra verificação
    valor_diferente >> executar_outra_escolha >> [valor_igual_2, valor_diferente_de_2]

    [ valor_igual_2, valor_diferente_de_2] >> fim

execucao_dag = execucao_dag()