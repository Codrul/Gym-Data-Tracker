import gspread 
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine

def connect_sheets():
# Google Sheets API setup
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    CREDENTIALS = Credentials.from_service_account_file(
        "C:\\Users\\aditz\\Gym_Data_Tracker\\APIs\\grand-strand-465118-v6-5c8c40adf654.json",
        scopes=SCOPES
    )
    gc = gspread.authorize(CREDENTIALS)
    return gc

def connect_db():
# PostgreSQL connection
    with open(r'C:\\Users\\aditz\Gym_Data_Tracker\\APIs\\pgpass.txt', 'r') as f:
        db_pass = f.read().strip()

    engine = create_engine(
        f'postgresql+psycopg2://postgres:{db_pass}@localhost:5432/gym_data'
    )
    return engine
