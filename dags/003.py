from datetime import datetime
from airflow.sdk import dag, task
import random
import time
import pandas as pd
from pathlib import Path
import os

# ---------------------------------------------------------------------------
# Constantes e Configurações
# ---------------------------------------------------------------------------
TOTAL_DE_PESSOAS = 5000
LOTE_TAMANHO = 100
DATA_DIR = Path("/opt/airflow/data")

# ---------------------------------------------------------------------------
# Simulações auxiliares
# ---------------------------------------------------------------------------

def _simular_insercao_ldap(pessoa):
    """Simula a inserção de uma pessoa em um diretório LDAP."""
    time.sleep(0.1)
    return {"id": pessoa["id"], "status": "inserido"}

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="dag_003",
    description="Processa dados em lotes usando a estratégia de múltiplos arquivos Parquet.",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,

    # Limita o número total de tarefas ativas desta DAG, mesmo que o pool permita mais.
    max_active_tasks=5,

    # Limita a 1 execução ativa para cada instância da DAG
    max_active_runs=1,

    tags=["2025"],
)
def simular_insercao_ldap_dag_parquet():
    """
    Esta DAG demonstra um padrão de alta performance para processamento em larga escala:
    1.  Gera os dados e os escreve diretamente em múltiplos arquivos Parquet (um por lote).
    2.  A segunda tarefa apenas lista os caminhos para esses arquivos.
    3.  As tarefas paralelas recebem um caminho de arquivo, lendo um lote inteiro de uma vez.
        Isso minimiza o I/O e elimina a necessidade de "pular" linhas (skiprows).
    """

    @task
    def gerar_arquivos_de_lotes_task():
        """
        Gera os dados e os salva diretamente em múltiplos arquivos Parquet, um para cada lote.
        Retorna o caminho do diretório onde os lotes foram salvos.
        """
        # Cria um subdiretório único para esta execução para evitar conflitos.
        run_dir = DATA_DIR / f"run_{{{{ ts_nodash }}}}"
        run_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"Gerando {TOTAL_DE_PESSOAS} registros em lotes de {LOTE_TAMANHO}...")
        pessoas = [
            {"id": i, "nome": f"Pessoa {i}", "email": f"pessoa{i}@exemplo.com"}
            for i in range(1, TOTAL_DE_PESSOAS + 1)
        ]

        for i, offset in enumerate(range(0, len(pessoas), LOTE_TAMANHO)):
            lote = pessoas[offset : offset + LOTE_TAMANHO]
            df_lote = pd.DataFrame(lote)
            caminho_lote = run_dir / f"lote_{i}.parquet"
            df_lote.to_parquet(caminho_lote, engine='pyarrow', index=False)

        print(f"✅ Lotes salvos no diretório: {run_dir}")
        return str(run_dir)

    @task
    def listar_lotes_task(diretorio_lotes: str):
        """
        Lista todos os arquivos de lote (.parquet) no diretório especificado.
        """
        print(f"Listando arquivos de lote em: {diretorio_lotes}")
        path_obj = Path(diretorio_lotes)
        lista_de_arquivos = [str(p) for p in path_obj.glob("*.parquet")]
        print(f"✅ {len(lista_de_arquivos)} lotes encontrados.")
        return lista_de_arquivos

    @task(pool="ldap_pool")
    def processar_lote_parquet_task(caminho_arquivo_lote: str):
        """
        Recebe o caminho para UM arquivo de lote, o lê por inteiro e processa os dados.
        """
        print(f"Processando lote do arquivo: {os.path.basename(caminho_arquivo_lote)}")
        
        df_lote = pd.read_parquet(caminho_arquivo_lote, engine='pyarrow')

        resultados_lote = [
            _simular_insercao_ldap(pessoa) 
            for pessoa in df_lote.to_dict('records')
        ]
        
        return {"total_processado": len(resultados_lote), "status": "sucesso"}

    @task
    def consolidar_resultados_task(resultados_lotes):
        """Agrega os resultados de todos os lotes processados."""
        total_geral = sum(r["total_processado"] for r in resultados_lotes)
        print(f"🎉 Consolidação final: {total_geral} pessoas inseridas com sucesso no LDAP.")
        return {"total_final": total_geral}

    # Define a ordem de execução das tarefas
    diretorio_dos_lotes = gerar_arquivos_de_lotes_task()
    lista_de_lotes = listar_lotes_task(diretorio_dos_lotes)
    resultados = processar_lote_parquet_task.expand(caminho_arquivo_lote=lista_de_lotes)
    consolidar_resultados_task(resultados)


simular_insercao_ldap_dag_parquet()