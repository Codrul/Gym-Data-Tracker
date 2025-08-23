import sys
sys.path.insert(0, "/usr/local/airflow")
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
    dag_id="etl_load_to_3nf",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule_interval=None,
    catchup=False,
    tags=["ETL"],
    max_active_tasks=16,
    concurrency=5
) as dag:

    t1 = PythonOperator(
        task_id="load_exercises_3nf",
        python_callable=lambda: call_procedure("load_exercises_to_3nf")
    )

    t2 = PythonOperator(
        task_id="load_muscles_3nf",
        python_callable=lambda: call_procedure("load_muscles_to_3nf")
    )

    t3 = PythonOperator(
        task_id="load_resistances_3nf",
        python_callable=lambda: call_procedure("load_resistance_types_to_3nf")
    )

    t4 = PythonOperator(
        task_id="load_exercise_muscle_3nf",
        python_callable=lambda: call_procedure("load_exercise_muscle_to_3nf")
    )

    t5 = PythonOperator(
        task_id="load_dates",
        python_callable=lambda: call_procedure("load_dates")
    )
    t6 = PythonOperator(
        task_id="partition_fact",
        python_callable=lambda: call_procedure("bl_3nf.manage_fact_workouts_partitions")
    )

    t7 = PythonOperator(
        task_id="load_workouts_3nf",
        python_callable=lambda: call_procedure("load_workouts_to_3nf")
    )

    notify = EmailOperator(
        task_id="notify_me",
        to="codreanu.andrei1125@gmail.com",
        subject="ETL completed",
        html_content="<h3> Your ETL pipeline has finished successfully! </h3>"
    )
    # dependencies 

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> notify
