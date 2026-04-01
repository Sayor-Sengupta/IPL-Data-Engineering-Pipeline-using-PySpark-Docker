from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="ipl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    bronze = BashOperator(
        task_id="bronze_layer",
        bash_command="""
        docker exec spark-master \
        /opt/spark/bin/spark-submit \
        /opt/spark-app/jobs/bronze_jobs.py
        """
    )

    silver = BashOperator(
        task_id="silver_layer",
        bash_command="""
        docker exec spark-master \
        /opt/spark/bin/spark-submit \
        /opt/spark-app/jobs/silver_jobs.py
        """
    )

    gold = BashOperator(
        task_id="gold_layer",
        bash_command="""
        docker exec spark-master \
        /opt/spark/bin/spark-submit \
        /opt/spark-app/jobs/gold_jobs.py
        """
    )

    bronze >> silver >> gold