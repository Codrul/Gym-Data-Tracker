import gspread
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine
import os
import json
import logging
from airflow.hooks.base import BaseHook

# airflow/task logger
logger = logging.getLogger("airflow.task")


def connect_sheets():
    """
    Connect to Google Sheets using either a GitHub secret (API_KEY) or a local JSON file.
    """
    api_key = os.environ.get("API_KEY")
    if api_key:
        creds_dict = json.loads(api_key)  # API_KEY from secrets as JSON string
        logger.info("[connect_sheets] Using API_KEY from environment")
    else:
        # Fallback to local file
        local_path = r"C:\\Users\\aditz\\Gym_Data_Tracker\\APIs\\grand-strand-465118-v6-5c8c40adf654.json"
        with open(local_path, "r") as f:
            creds_dict = json.load(f)
        logger.info(f"[connect_sheets] Using local credentials file: {local_path}")

    # Google Sheets API setup
    scope = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
    gc = gspread.authorize(creds)
    return gc


def connect_db():
    """
    Connect to PostgreSQL using either an Airflow connection, environment variables, or a local secrets file.
    """
    engine = None

    # Try Airflow connection first
    # to do: figure out why airflow doesnt have gym_db as a variable 
    # it seems we are always using the fallback logic in our DAGs
    try:
        conn = BaseHook.get_connection("gym_db")
        engine = create_engine(conn.get_uri())
        logger.info("[connect_db] Using Airflow connection: gym_db")
    except Exception:
        logger.info("[connect_db] Airflow connection 'gym_db' not found, falling back to local/secret logic")

    # ok so this should actually use my airflow metadata
    airflow_conn = os.environ.get("AIRFLOW_CONN_GYM_DB")
    if airflow_conn:
        engine = create_engine(airflow_conn)
        logger.info("[connect_db] Using AIRFLOW_CONN_GYM_DB from env")
        return engine

    # Fallback to local/secret logic if Airflow connection not found
    if engine is None:
        db_password = os.environ.get("DB_PASSWORD")
        if not db_password:
            local_pass_file = r'C:\\Users\\aditz\\Gym_Data_Tracker\\APIs\\pgpass.txt'
            with open(local_pass_file, 'r') as f:
                db_password = f.read().strip()
            logger.info(f"[connect_db] Using local password file: {local_pass_file}")

        db_host = "host.docker.internal" if os.environ.get("IN_DOCKER") == "1" else "192.168.1.178"
        engine = create_engine(f'postgresql+psycopg2://postgres:{db_password}@{db_host}:5432/gym_data')
        logger.info(f"[connect_db] Using fallback connection: postgres://postgres@{db_host}:5432/gym_data")

    return engine

