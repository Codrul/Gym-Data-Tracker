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

# import my hard work
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
    schedule="@daily",
    catchup=False,
    tags=["ETL"],
    max_active_tasks=16
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

    trigger_load_to_cl = TriggerDagRunOperator(
        task_id ="trigger_load_to_cl",
        trigger_dag_id="etl_load_to_cleansing",
        wait_for_completion=False
    )
    

# list all tasks that should run in parallel
extract_tasks = [t1, t2, t3, t4, t5]

# set the TriggerDagRunOperator to run after all extract tasks
for t in extract_tasks:
    t >> trigger_load_to_cl # I swear this looks like a for loop but apparently in airflow it should run the 
                            # tasks in parallel and then the last one after the first ones are finished
