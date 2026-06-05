#!/bin/bash
set -euo pipefail

docker run --rm \
  --network practica_creativa_default \
  -v /home/casuario/Desktop/practica-creativa-global/practica_creativa:/workspace \
  -w /workspace \
  -e PYSPARK_PYTHON=python3 \
  -e AWS_ACCESS_KEY_ID=admin \
  -e AWS_SECRET_ACCESS_KEY=password \
  -e AWS_REGION=eu-south-2 \
  -e AWS_DEFAULT_REGION=eu-south-2 \
  -e HOME=/tmp \
  spark-lakehouse-runtime:4.1.2 \
  /bin/bash -lc '/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client /workspace/resources/train_model_lakehouse.py'

  