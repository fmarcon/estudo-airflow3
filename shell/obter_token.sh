#!/usr/bin/env bash

ENDPOINT_URL="http://localhost:8080"
curl -X POST ${ENDPOINT_URL}/auth/token \
  -H "Content-Type: application/json" \
  -d '{
    "username": "airflow",
    "password": "airflow"
  }'