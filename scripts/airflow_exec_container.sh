servico=airflow-$1 
echo Container $servico:
docker compose exec -it -u airflow $servico /bin/bash