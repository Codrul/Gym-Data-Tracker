import sys
sys.path.insert(0, "/usr/local/airflow")
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

# import your modules
import DWH.extract_scripts.connect as c
import DWH.extract_scripts.extract_exercise_muscles as eem
import DWH.extract_scripts.extract_exercises as ee
import DWH.extract_scripts.extract_muscles as em
import DWH.extract_scripts.extract_resistance_types as ert
import DWH.extract_scripts.extract_workouts as ew

# default args
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": ["codreanu.andrei1125@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

local_tz = pendulum.timezone("Europe/Bucharest")

with DAG(
    dag_id="etl_gym_data",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule_interval="0 1 * * *",
    catchup=False,
    tags=["etl"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_exercises",
        python_callable=lambda **kwargs: ee.load_exercises(c.connect_sheets(), c.connect_db(), [], []),
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="extract_muscles",
        python_callable=lambda **kwargs: em.load_muscles(c.connect_sheets(), c.connect_db(), [], []),
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="extract_resistance_types",
        python_callable=lambda **kwargs: ert.load_resistance_types(c.connect_sheets(), c.connect_db(), [], []),
        provide_context=True,
    )

    t4 = PythonOperator(
        task_id="extract_exercise_muscle",
        python_callable=lambda **kwargs: eem.load_exercise_muscle(c.connect_sheets(), c.connect_db(), [], []),
        provide_context=True,
    )

    t5 = PythonOperator(
        task_id="extract_workouts",
        python_callable=lambda **kwargs: ew.load_workouts(c.connect_sheets(), c.connect_db(), [], []),
        provide_context=True,
    )

    notify = EmailOperator(
        task_id="notify",
        to="codreanu.andrei1125@gmail.com",
        subject="ETL Pipeline Completed",
        html_content="The daily ETL pipeline has finished running successfully.",
    )

    # dependencies
    t1 >> t2 >> t3 >> t4 >> t5 >> notify

