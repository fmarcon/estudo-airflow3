servico=airflow_$1 
echo Container $servico:
docker exec -it -u airflow $servico /bin/bash