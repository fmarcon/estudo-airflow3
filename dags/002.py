from time import sleep

from airflow import DAG
from airflow.decorators import task

with DAG(
    "dag_002",
    start_date=None,
    catchup=False,
    tags=["2025"],
) as dag:

    @task()
    def dag001() -> None:
        """DAG de estudo 001."""
        return list(range(1, 6))

    #paginas = dag001()

if __name__ == "__main__":
    dag.test()