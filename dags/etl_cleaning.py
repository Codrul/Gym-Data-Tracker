import sys
import os
# dynamically set python root so my modules get read
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models.baseoperator import chain
from airflow.sensors.external_task import ExternalTaskSensor
from sqlalchemy import text
import DWH.extract_scripts.connect as c


logger = LoggingMixin().log
local_tz = pendulum.timezone("Europe/Bucharest")

def call_procedure(proc_name):
    """Generic function to call a PostgreSQL stored procedure."""
    logger.info("Starting procedure: %s", proc_name)
    engine = c.connect_db()
    with engine.begin() as conn:
        conn.execute(text(f"CALL {proc_name}();"))
    logger.info("Finished procedure: %s", proc_name)

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email": ["codreanu.andrei1125@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="etl_cleansing",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule=None,
    catchup=False,
    tags=["ETL"],
    max_active_tasks=16
) as dag:

    t1 = PythonOperator(
        task_id="clean_exercises",
        python_callable=lambda: call_procedure("clean_exercises")
    )

    t2 = PythonOperator(
        task_id="clean_muscles",
        python_callable=lambda: call_procedure("clean_muscles")
    )

    t3 = PythonOperator(
        task_id="clean_exercise_muscle",
        python_callable=lambda: call_procedure("clean_exercise_muscle")
    )

    t4 = PythonOperator(
        task_id="clean_resistance_types",
        python_callable=lambda: call_procedure("clean_resistance_types")
    )

    t5 = PythonOperator(
        task_id="clean_workouts",
        python_callable=lambda: call_procedure("clean_workouts")
    )

    trigger_3nf = TriggerDagRunOperator(
        task_id ="trigger_load_to_3nf",
        trigger_dag_id="etl_load_to_3nf",
        wait_for_completion=False
    )

    # dependencies 

    t1 >> t2 >> t3 >> t4 >> t5 >> trigger_3nf

