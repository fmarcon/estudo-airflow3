#/bin/bash

docker exec -it airflow-scheduler \
  airflow db clean --skip-archive -y \
    --clean-before-timestamp "$(date +"%Y-%m-%d %H:%M:%S%:z")" #--dry-run