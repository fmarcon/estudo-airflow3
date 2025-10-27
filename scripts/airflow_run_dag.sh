comando=test
docker compose exec -it -u airflow airflow-worker airflow dags $comando $1
