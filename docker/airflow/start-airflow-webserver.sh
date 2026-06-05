#!/usr/bin/env bash
set -e

airflow db migrate

airflow connections delete spark_default || true
airflow connections add spark_default \
  --conn-type spark \
  --conn-host spark://spark-master \
  --conn-port 7077 \
  --conn-extra '{"deploy-mode":"client","spark-binary":"spark-submit"}'

exec airflow webserver