import gspread
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine
import os
import json

def connect_sheets():
    # Try GitHub secret first
    api_key = os.environ.get("API_KEY")
    if api_key:
        creds_dict = json.loads(api_key)  # API_KEY from secrets as JSON string
    else:
        # Fallback to local file
        with open(r"C:\\Users\\aditz\\Gym_Data_Tracker\\APIs\\grand-strand-465118-v6-5c8c40adf654.json", "r") as f:
            creds_dict = json.load(f)

    # Google Sheets API setup
    scope = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
    gc = gspread.authorize(creds)
    return gc

def connect_db():
    # Try GitHub secret first
    db_password = os.environ.get("DB_PASSWORD")
    if not db_password:
        # Fallback to local file
        with open(r'C:\\Users\\aditz\\Gym_Data_Tracker\\APIs\\pgpass.txt', 'r') as f:
            db_password = f.read().strip()

    # Decide host based on environment
    if os.environ.get("IN_DOCKER") == "1":
        db_host = "host.docker.internal"  # Docker container connecting to host
    else:
        db_host = "localhost"  # Running locally

    # PostgreSQL connection
    engine = create_engine(
        f'postgresql+psycopg2://postgres:{db_password}@{db_host}:5432/gym_data'
    )
    return engine
