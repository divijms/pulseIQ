from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "pulseiq",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pulseiq_hourly_batch_aggregation",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["pulseiq", "batch", "analytics"],
) as dag:

    run_hourly_aggregation = BashOperator(
        task_id="run_hourly_gold_aggregation",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/ivy \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  /opt/spark/work-dir/processing/batch/gold_to_hourly_analytics.py
        """,
    )

    run_hourly_aggregation

