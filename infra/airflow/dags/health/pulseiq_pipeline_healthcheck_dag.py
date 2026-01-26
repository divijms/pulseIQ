from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

BRONZE_PATH = "/tmp/pulseiq/bronze/events"
SILVER_PATH = "/tmp/pulseiq/silver/events"
GOLD_PATH   = "/tmp/pulseiq/gold/metrics"

FRESHNESS_THRESHOLD_MINUTES = 15

default_args = {
    "owner": "pulseiq",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="pulseiq_pipeline_healthcheck",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["pulseiq", "healthcheck"],
) as dag:

    bronze_check = BashOperator(
        task_id="check_bronze_freshness",
        bash_command=f"""
            docker exec spark-master /opt/spark/bin/spark-submit \
            --conf spark.jars.ivy=/tmp/ivy \
            --packages io.delta:delta-spark_2.12:3.2.0 \
            /opt/spark/work-dir/processing/batch/healthcheck_delta_freshness.py \
            {BRONZE_PATH} BRONZE {FRESHNESS_THRESHOLD_MINUTES}
            """,)

    silver_check = BashOperator(
        task_id="check_silver_freshness",
        bash_command=f"""
    docker exec spark-master /opt/spark/bin/spark-submit \
      --conf spark.jars.ivy=/tmp/ivy \
      --packages io.delta:delta-spark_2.12:3.2.0 \
      /opt/spark/work-dir/processing/batch/healthcheck_delta_freshness.py \
      {SILVER_PATH} SILVER {FRESHNESS_THRESHOLD_MINUTES}
    """,)

    gold_check = BashOperator(
        task_id="check_gold_freshness",
        bash_command=f"""
    docker exec spark-master /opt/spark/bin/spark-submit \
      --conf spark.jars.ivy=/tmp/ivy \
      --packages io.delta:delta-spark_2.12:3.2.0 \
      /opt/spark/work-dir/processing/batch/healthcheck_delta_freshness.py \
      {GOLD_PATH} GOLD {FRESHNESS_THRESHOLD_MINUTES}
    """,)

    bronze_check >> silver_check >> gold_check

