FROM spark-lakehouse-runtime:4.1.2

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    procps \
    curl \
    ca-certificates \
    gcc \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${JAVA_HOME}/bin:${PATH}"

RUN pip uninstall -y pyspark || true

ARG AIRFLOW_VERSION=2.10.4
ARG PYTHON_VERSION=3.11

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    apache-airflow==${AIRFLOW_VERSION} \
    apache-airflow-providers-apache-spark \
    psycopg2-binary \
    boto3 \
    mlflow