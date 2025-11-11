#/bin/bash

data="$(date -u -d '-1 hour' '+%Y-%m-%dT%H:%M:%S')"
echo "Limpando dados anteriores a: $data (UTC)"

docker exec -it airflow-worker airflow db clean \
  --clean-before-timestamp $data \
  --verbose \
  -y \
  --skip-archive

#docker exec airflow-worker airflow db clean --clean-before-timestamp 20251108 --yes

