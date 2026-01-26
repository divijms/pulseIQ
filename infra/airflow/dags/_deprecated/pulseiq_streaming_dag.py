from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "pulseiq",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="pulseiq_streaming_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    start_bronze = BashOperator(
        task_id="start_bronze",
        bash_command="""
        spark-submit \
          --master spark://spark-master:7077 \
          --conf spark.jars.ivy=/tmp/ivy \
          --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
          /opt/airflow/dags/../../processing/bronze/kafka_to_bronze.py
        """,
    )

    start_silver = BashOperator(
        task_id="start_silver",
        bash_command="""
        spark-submit \
          --master spark://spark-master:7077 \
          --conf spark.jars.ivy=/tmp/ivy \
          --packages io.delta:delta-spark_2.12:3.2.0 \
          /opt/airflow/dags/../../processing/silver/bronze_to_silver.py
        """,
    )

    start_gold = BashOperator(
        task_id="start_gold",
        bash_command="""
        spark-submit \
          --master spark://spark-master:7077 \
          --conf spark.jars.ivy=/tmp/ivy \
          --packages io.delta:delta-spark_2.12:3.2.0 \
          /opt/airflow/dags/../../processing/gold/silver_to_gold.py
        """,
    )

    start_anomaly = BashOperator(
        task_id="start_anomaly",
        bash_command="""
        spark-submit \
          --master spark://spark-master:7077 \
          --conf spark.jars.ivy=/tmp/ivy \
          --packages io.delta:delta-spark_2.12:3.2.0 \
          /opt/airflow/dags/../../processing/anomaly/gold_anomaly_detection.py
        """,
    )

    start_bronze >> start_silver >> start_gold >> start_anomaly

