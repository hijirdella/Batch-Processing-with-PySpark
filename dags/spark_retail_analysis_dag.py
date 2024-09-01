from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import os


default_args = {
    "owner": "dibimbing",
    "retry_delay": timedelta(minutes=5),
}

spark_dag = DAG(
    dag_id="spark_retail_analysis_dag",
    default_args=default_args,
    schedule_interval=None,  # Set to None for manual trigger
    dagrun_timeout=timedelta(minutes=60),
    description="DAG for retail data analysis using Spark",
    start_date=days_ago(1),
    catchup=False,
)

spark_analysis = SparkSubmitOperator(
    application="/spark-scripts/spark-retail-analysis.py",  # Path to your Spark script
    conn_id="spark_main",
    task_id="spark_retail_analysis_task",
    dag=spark_dag,
    packages="org.postgresql:postgresql:42.2.18"
)

spark_analysis
