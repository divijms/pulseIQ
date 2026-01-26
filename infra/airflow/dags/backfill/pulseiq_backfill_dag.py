from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Param
from datetime import datetime, timedelta

default_args = {
    "owner": "pulseiq",
    "retries": 0,
}

with DAG(
    dag_id="pulseiq_backfill_replay",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # manual only
    catchup=False,
    default_args=default_args,
    params={
        "start_ts": Param("2025-01-20T00:00:00", type="string"),
        "end_ts": Param("2025-01-20T23:59:59", type="string"),
    },
    tags=["pulseiq", "backfill", "replay"],
) as dag:

    backfill = BashOperator(
        task_id="run_backfill",
        bash_command="""
docker exec spark-master /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/ivy \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  /opt/spark/work-dir/processing/batch/backfill_analytics_and_alerts.py \
  {{ params.start_ts }} {{ params.end_ts }}
""",)


    backfill

