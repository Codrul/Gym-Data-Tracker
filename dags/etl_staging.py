import sys 
sys.path.insert(0, "/usr/local/airflow")
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models.baseoperator import chain

# import your modules
import DWH.extract_scripts.connect as c
import DWH.extract_scripts.extract_exercise_muscles as eem
import DWH.extract_scripts.extract_exercises as ee
import DWH.extract_scripts.extract_muscles as em
import DWH.extract_scripts.extract_resistance_types as ert
import DWH.extract_scripts.extract_workouts as ew

# airflow logger
logger = LoggingMixin().log

# Python callables for operators
def extract_exercises_callable():
    logger.info("Starting extract_exercises task...")
    result = ee.load_exercises(c.connect_sheets(), c.connect_db(), [], [])
    logger.info("Finished extract_exercises task with result: %s", result)
    return result

def extract_muscles_callable():
    logger.info("Starting extract_muscles task...")
    result = em.load_muscles(c.connect_sheets(), c.connect_db(), [], [])
    logger.info("Finished extract_muscles task with result: %s", result)
    return result

def extract_resistance_types_callable():
    logger.info("Starting extract_resistance_types task...")
    result = ert.load_resistance_types(c.connect_sheets(), c.connect_db(), [], [])
    logger.info("Finished extract_resistance_types task with result: %s", result)
    return result

def extract_exercise_muscle_callable():
    logger.info("Starting extract_exercise_muscle task...")
    result = eem.load_exercise_muscle(c.connect_sheets(), c.connect_db(), [], [])
    logger.info("Finished extract_exercise_muscle task with result: %s", result)
    return result

def extract_workouts_callable():
    logger.info("Starting extract_workouts task...")
    result = ew.load_workouts(c.connect_sheets(), c.connect_db(), [], [])
    logger.info("Finished extract_workouts task with result: %s", result)
    return result

# default args
default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email": ["codreanu.andrei1125@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

local_tz = pendulum.timezone("Europe/Bucharest")

with DAG(
    dag_id="etl_extract_to_staging",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule_interval="0 1 * * *",
    catchup=False,
    tags=["ETL"],
    max_active_tasks=16,
    concurrency=6
) as dag:
 
    t1 = PythonOperator(
        task_id="extract_exercises",
        python_callable=extract_exercises_callable,
    )

    t2 = PythonOperator(
        task_id="extract_muscles",
        python_callable=extract_muscles_callable,
    )

    t4 = PythonOperator(
        task_id="extract_exercise_muscle",
        python_callable=extract_exercise_muscle_callable,
    )

    t3 = PythonOperator(
        task_id="extract_resistance_types",
        python_callable=extract_resistance_types_callable,
    )

    t5 = PythonOperator(
        task_id="extract_workouts",
        python_callable=extract_workouts_callable,
    )

    notify = EmailOperator(
        task_id="notify",
        to="codreanu.andrei1125@gmail.com",
        subject="ETL Pipeline Completed",
        html_content="The staging_layer load has finished running successfully.",
    )

    # dependencies
    first_half = [t1, t2, t3]
    second_half = [t4, t5]
    chain(*first_half, *second_half, notify)

