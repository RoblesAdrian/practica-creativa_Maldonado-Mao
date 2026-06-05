from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="train_model_lakehouse_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark"],
) as dag:
    train_model = BashOperator(
        task_id="train_model",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--deploy-mode client "
            "/workspace/resources/train_model_lakehouse.py"
        ),
    )