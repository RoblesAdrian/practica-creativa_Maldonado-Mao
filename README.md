# Practica Creativa Lakehouse

This project is a local lakehouse-style environment for training and running Spark jobs with Kafka, Cassandra, MinIO, Iceberg, Airflow, MLflow, and a small web app. It also includes a minimal Prometheus + Grafana monitoring setup for host and container metrics [web:528][web:471].

## What this project contains

- Spark standalone cluster and Spark submit runtime.
- Kafka broker plus init script.
- Cassandra plus init script.
- MinIO plus bucket bootstrap.
- Iceberg REST catalog.
- Airflow webserver, scheduler, and init bootstrap.
- MLflow tracking server.
- A FastAPI spark runner service used by Airflow.
- A Flask web app.
- Prometheus + Grafana monitoring. [web:528][web:530]

## Requirements

- Docker and Docker Compose.
- Git.
- Java and SBT for the Scala Spark project build.
- Enough disk space for the Docker images and generated artifacts. [web:529][web:532]

## Clone the repository

```bash
git clone https://github.com/RoblesAdrian/practica-creativa_Maldonado-Mao.git
cd practica_creativa
```

## Recreate the omitted large files

Some files are build artifacts and should be recreated locally after cloning. Build artifacts are commonly kept out of Git because they bloat the repository and are often regenerated from source [web:523][web:526].

### 1) Recreate the Spark assembly JAR

This project expects:

```text
flight_prediction/target/scala-2.13/flight_prediction-assembly-0.1.jar
```

Build it from the Scala project:

```bash
cd flight_prediction
sbt clean assembly
cd ..
```

`SBT assembly` creates a fat JAR in the `target/scala-2.13/` tree, which is the normal output for this kind of project [web:529][web:532][web:533].

### 2) Recreate the Spark dependency JARs

This project also expects these jars in `docker/spark-submit/jars/`:

```text
bundle-2.24.6.jar
hadoop-aws-3.4.1.jar
iceberg-spark-runtime-4.0_2.13-1.10.0.jar
```

If they are not present after cloning, recreate the directory and copy the jars back into place from your local cache or download source:

```bash
mkdir -p docker/spark-submit/jars
cp /path/to/bundle-2.24.6.jar docker/spark-submit/jars/
cp /path/to/hadoop-aws-3.4.1.jar docker/spark-submit/jars/
cp /path/to/iceberg-spark-runtime-4.0_2.13-1.10.0.jar docker/spark-submit/jars/
```

If your Dockerfile downloads them during build, you only need the directory to exist before building the image.

## Make scripts executable

```bash
chmod +x cassandra-init/init.sh
chmod +x mongo-init/01-import-distances.sh
chmod +x resources/*.sh
```

If you use the FastAPI spark runner, also ensure the wrapper script is executable:

```bash
chmod +x resources/run_train_model.sh
```

## Build the images

```bash
docker compose build
```

If you want a completely fresh rebuild:

```bash
docker compose build --no-cache
```

## Start the stack

```bash
docker compose up -d
```

For a clean first start, especially after fixing DB or init issues:

```bash
docker compose down -v
docker compose up -d
```

Docker Compose is the right tool here because it lets you define service dependencies and start the whole environment together [web:530][web:528].

## Wait for the init containers

These services should complete before you try to use the stack:

```bash
docker compose logs -f cassandra-init
docker compose logs -f mc-init
docker compose logs -f kafka-init
docker compose logs -f airflow-init
```

The Airflow setup relies on the metadata database being initialized before the scheduler/webserver start, and healthchecks are the standard way to make Compose wait for a service to become usable [web:530][web:528][web:440].

## Useful services and URLs

- Airflow: `http://localhost:8085`
- Spark master UI: `http://localhost:8080`
- Spark worker 1 UI: `http://localhost:8081`
- Spark worker 2 UI: `http://localhost:8082`
- MinIO console: `http://localhost:9001`
- Iceberg REST: `http://localhost:8181`
- Kafka: `localhost:9092`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`
- cAdvisor: `http://localhost:8088`
- node-exporter: `http://localhost:9100` [web:471][web:474]

## Airflow admin user

If the admin user is not created automatically, run:

```bash
docker exec -it airflow-webserver bash
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
```

If your `airflow-init` container is healthy, you should normally only need this once.

## Run the Spark jobs manually

### Load data into the lakehouse

```bash
docker compose run --rm spark-submit-load-train-data
```

### Train the model

```bash
docker compose run --rm spark-submit-train
```

### Run the prediction stream

```bash
docker compose run --rm spark-submit-predict
```

## Run the Airflow DAG

The Airflow DAG triggers the FastAPI spark runner service, which then starts the Spark training job in the background.

```bash
docker compose up -d
```

Then in Airflow:
1. Open the UI.
2. Enable the DAG.
3. Trigger `train_model_lakehouse_dag`.

If you want to test the FastAPI endpoint directly from the Airflow container:

```bash
docker exec -it airflow-webserver bash
curl -X POST http://spark-runner:8000/run-train-model
```

## Run the spark runner service

If the runner service is part of your Compose setup, start it with:

```bash
docker compose up -d spark-runner
docker compose logs -f spark-runner
```

It listens on `POST /run-train-model` and launches the training script for you.

## Monitoring setup

The monitoring stack is intentionally minimal:

- Prometheus scrapes container and host metrics.
- Grafana visualizes them.
- cAdvisor exposes Docker container metrics.
- node-exporter exposes host metrics. [web:471][web:474]

Start it with:

```bash
docker compose up -d node-exporter cadvisor prometheus grafana
```

### Good starter Grafana panels

Use these two panels first:
- CPU usage by container.
- Memory usage by container.

They are easy to understand and still show whether Spark, Airflow, Kafka, MinIO, or MLflow are actually doing work [web:471][web:489].

## Build and runtime commands

### Inspect running services

```bash
docker compose ps
```

### Follow logs

```bash
docker compose logs -f airflow-webserver
docker compose logs -f airflow-scheduler
docker compose logs -f spark-submit-train
docker compose logs -f mlflow
```

### Stop everything

```bash
docker compose down
```

### Remove volumes too

```bash
docker compose down -v
```

## Git ignore rules

Do not commit generated artifacts such as:

```gitignore
flight_prediction/target/
docker/spark-submit/jars/
*.jar
airflow/logs/
airflow/dags/__pycache__/
resources/__pycache__/
```

Build artifacts and `target/` folders should generally be excluded from Git and recreated locally when needed [web:523][web:526][web:529].

## If GitHub rejects a push

If large binaries were already committed, GitHub will reject the push until they are removed from repository history. In that case, rewrite the history to remove the large files, then force-push the cleaned branch [web:519][web:522][web:525].

## One-command setup summary

After cloning, the typical setup is:

```bash
git clone https://github.com/RoblesAdrian/practica-creativa_Maldonado-Mao.git
cd practica_creativa
mkdir -p docker/spark-submit/jars
cp /path/to/bundle-2.24.6.jar docker/spark-submit/jars/
cp /path/to/hadoop-aws-3.4.1.jar docker/spark-submit/jars/
cp /path/to/iceberg-spark-runtime-4.0_2.13-1.10.0.jar docker/spark-submit/jars/
cd flight_prediction
sbt clean assembly
cd ..
chmod +x cassandra-init/init.sh mongo-init/01-import-distances.sh resources/*.sh
docker compose build
docker compose up -d
```

## Notes

This repository is designed to be reproducible from source plus a small set of locally rebuilt artifacts. The safest workflow is to keep source files in Git, ignore build outputs, and rebuild the jars and container images after cloning [web:523][web:526][web:529].
