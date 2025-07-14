import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from sqlalchemy import (
    MetaData, create_engine, Table, Column,
    select, insert, String
)

# Google Sheets API setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
CREDENTIALS = Credentials.from_service_account_file(
    "C:\\Users\\aditz\\Gym_Data_Tracker\\APIs\\grand-strand-465118-v6-5c8c40adf654.json",
    scopes=SCOPES
)
gc = gspread.authorize(CREDENTIALS)

# PostgreSQL connection
with open('APIs/pgpass.txt', 'r') as f:
    db_pass = f.read().strip()

engine = create_engine(
    f'postgresql+psycopg2://postgres:{db_pass}@localhost:5432/gym_data'
)
metadata = MetaData()

# Table definitions
exercises = Table(
    'exercises', metadata,
    Column('exercise_id', String, primary_key=True),
    Column('exercise_name', String),
    Column('exercise_movement_type', String),
    Column('exercise_bodysplit', String),
    schema='staging_layer'
)

exercise_muscle = Table(
    'exercise_muscle', metadata,
    Column('exercise_id', String, primary_key=True),
    Column('exercise_name', String),
    Column('muscle_id', String),
    Column('muscle_name', String),
    Column('muscle_role', String),
    schema='staging_layer'
)

muscles = Table(
    'muscles', metadata,
    Column('muscle_id', String, primary_key=True),
    Column('muscle_name', String),
    Column('muscle_group', String),
    schema='staging_layer'
)

resistance_types = Table(
    'resistance_types', metadata,
    Column('resistance_id', String, primary_key=True),
    Column('resistance_type', String),
    Column('resistance_category', String),
    schema='staging_layer'
)


def load_exercises():
    try:
        spreadsheet = gc.open_by_key(
            '1OiufKuY1WB_-tzfvKWZh9OHeCCEX81jQ1KHuNE5lZsQ'
        )
        worksheet = spreadsheet.worksheet('Exercises')
        exercise_table = worksheet.get_all_records()
    except Exception as e:
        print(f'Error {e} occurred. Failed to load from Google Sheets')
        return

    df = pd.DataFrame(exercise_table)
    column_mapping = {
        'ID': 'exercise_id',
        'Name': 'exercise_name',
        'Movement type': 'exercise_movement_type',
        'Upper/Lower': 'exercise_bodysplit'
    }
    df.rename(columns=column_mapping, inplace=True)

    exercise_df = df[
        ['exercise_id', 'exercise_name',
         'exercise_movement_type', 'exercise_bodysplit']
    ]

    try:
        inserted_rows = 0
        with engine.connect() as conn:
            for row in exercise_df.itertuples(index=False):
                exists_stmt = select(exercises.c.exercise_id).where(
                    exercises.c.exercise_id == str(row.exercise_id)
                )
                result = conn.execute(exists_stmt).fetchone()
                if result is None:
                    ins_stmt = insert(exercises).values(
                        exercise_id=row.exercise_id,
                        exercise_name=row.exercise_name,
                        exercise_movement_type=row.exercise_movement_type,
                        exercise_bodysplit=row.exercise_bodysplit
                    )
                    conn.execute(ins_stmt)
                    inserted_rows += 1
            conn.commit()
        return (
            f"{inserted_rows} rows have been loaded "
            "into *staging_layer.exercises*"
        )
    except Exception as e:
        print(f'Error {e} occurred. Could not insert into the exercises table')


def load_exercise_muscle():
    try:
        spreadsheet = gc.open_by_key(
            '1OiufKuY1WB_-tzfvKWZh9OHeCCEX81jQ1KHuNE5lZsQ'
        )
        worksheet = spreadsheet.worksheet('Exercise_Muscle')
        exercise_muscle_table = worksheet.get_all_records()
    except Exception as e:
        print(f'Error {e} occurred. Failed to load from Google Sheets')
        return

    df = pd.DataFrame(exercise_muscle_table)
    column_mapping = {
        'Exercise_ID': 'exercise_id',
        'Ex_name': 'exercise_name',
        'Muscle_ID': 'muscle_id',
        'Musc_name': 'muscle_name',
        'Role': 'muscle_role'
    }
    df.rename(columns=column_mapping, inplace=True)

    df = df[
        ['exercise_id', 'exercise_name',
         'muscle_id', 'muscle_name', 'muscle_role']
    ]

    try:
        inserted_rows = 0
        with engine.connect() as conn:
            for row in df.itertuples(index=False):
                exists_stmt = select(exercise_muscle.c.exercise_id).where(
                    (exercise_muscle.c.exercise_id == str(row.exercise_id)) &
                    (exercise_muscle.c.muscle_id == str(row.muscle_id))
                )
                result = conn.execute(exists_stmt).fetchone()
                if result is None:
                    ins_stmt = insert(exercise_muscle).values(
                        exercise_id=row.exercise_id,
                        exercise_name=row.exercise_name,
                        muscle_id=row.muscle_id,
                        muscle_name=row.muscle_name,
                        muscle_role=row.muscle_role
                    )
                    conn.execute(ins_stmt)
                    inserted_rows += 1
            conn.commit()
        return (
            f"{inserted_rows} rows have been loaded "
            "into *staging_layer.exercise_muscle*"
        )
    except Exception as e:
        print(f'Error {e} occurred. Could not insert into exercise_muscle')


def load_muscles():
    try:
        spreadsheet = gc.open_by_key(
            '1OiufKuY1WB_-tzfvKWZh9OHeCCEX81jQ1KHuNE5lZsQ'
        )
        worksheet = spreadsheet.worksheet('Muscles')
        muscles_table = worksheet.get_all_records()
    except Exception as e:
        print(f'Error {e} occurred. Failed to load from Google Sheets')
        return

    df = pd.DataFrame(muscles_table)
    column_mapping = {
        'ID': 'muscle_id',
        'Muscle name': 'muscle_name',
        'Muscle groups': 'muscle_group',
    }
    df.rename(columns=column_mapping, inplace=True)

    df = df[
        ['muscle_id', 'muscle_name', 'muscle_group']
    ]

    try:
        inserted_rows = 0
        with engine.connect() as conn:
            for row in df.itertuples(index=False):
                exists_stmt = select(muscles.c.muscle_id).where(
                    (muscles.c.muscle_id == str(row.muscle_id)))
                result = conn.execute(exists_stmt).fetchone()
                if result is None:
                    ins_stmt = insert(muscles).values(
                        muscle_id=row.muscle_id,
                        muscle_name=row.muscle_name,
                        muscle_group=row.muscle_group,
                    )
                    conn.execute(ins_stmt)
                    inserted_rows += 1
            conn.commit()
        return (
            f"{inserted_rows} rows have been loaded "
            "into *staging_layer.muscle*"
        )
    except Exception as e:
        print(f'Error {e} occurred. Could not insert into muscle')


def load_resistance_types():
    try:
        spreadsheet = gc.open_by_key(
            '1OiufKuY1WB_-tzfvKWZh9OHeCCEX81jQ1KHuNE5lZsQ'
        )
        worksheet = spreadsheet.worksheet('Resistance_types')
        resistance_table = worksheet.get_all_records()
    except Exception as e:
        print(f'Error {e} occurred. Failed to load from Google Sheets')
        return

    df = pd.DataFrame(resistance_table)
    column_mapping = {
        'Resistance_ID': 'resistance_id',
        'Resistance': 'resistance_type',
        'Resistance_category': 'resistance_category',
    }
    df.rename(columns=column_mapping, inplace=True)

    df = df[
        ['resistance_id', 'resistance_type', 'resistance_category']
    ]

    try:
        inserted_rows = 0
        with engine.connect() as conn:
            for row in df.itertuples(index=False):
                exists_stmt = select(resistance_types.c.resistance_id).where(
                    (resistance_types.c.resistance_id == str(row.resistance_id)))
                result = conn.execute(exists_stmt).fetchone()
                if result is None:
                    ins_stmt = insert(resistance_types).values(
                        resistance_id=row.resistance_id,
                        resistance_type=row.resistance_type,
                        resistance_category=row.resistance_category,
                    )
                    conn.execute(ins_stmt)
                    inserted_rows += 1
            conn.commit()
        return (
            f"{inserted_rows} rows have been loaded "
            "into *staging_layer.resistance_types*"
        )
    except Exception as e:
        print(f'Error {e} occurred. Could not insert into muscle')


# don't forget to run these lol
print(load_exercises())
print(load_exercise_muscle())
print(load_muscles())
print(load_resistance_types())
