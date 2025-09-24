import pandas as pd
import logging
from sqlalchemy import MetaData, Table, Column, select, insert, String, DateTime, update

metadata = MetaData()

# Define the table
exercises = Table(
    'exercises', metadata,
    Column('exercise_id', String, primary_key=True),
    Column('exercise_name', String),
    Column('exercise_movement_type', String),
    Column('exercise_bodysplit', String),
    Column('created_at', DateTime),
    Column('updated_at', DateTime),
    schema='staging_layer'
)

# so I can log stuff
logger = logging.getLogger("airflow.task")

def load_exercises(gc, engine, success_msg, error_msg):
    """
    Extract exercises from Google Sheets and load/update into staging_layer.exercises
    """
    try:
        logger.info("[extract_exercises] Connecting to Google Sheets...")
        spreadsheet = gc.open_by_key('1OiufKuY1WB_-tzfvKWZh9OHeCCEX81jQ1KHuNE5lZsQ')
        worksheet = spreadsheet.worksheet('Exercises')
        exercise_table = worksheet.get_all_records()
        logger.info("[extract_exercises] Retrieved %s rows from Google Sheets", len(exercise_table))
    except Exception as e:
        error_text = f"[extract_exercises] Error {e} occurred. Failed to load from Google Sheets"
        error_msg.append(error_text)
        logger.exception(error_text)
        return

    # Load into DataFrame and rename columns
    df = pd.DataFrame(exercise_table)
    column_mapping = {
        'ID': 'exercise_id',
        'Name': 'exercise_name',
        'Movement type': 'exercise_movement_type',
        'Upper/Lower': 'exercise_bodysplit'
    }
    df.rename(columns=column_mapping, inplace=True)
    df['created_at'] = pd.Timestamp.now()
    df['updated_at'] = pd.Timestamp.now()

    exercise_df = df[[
        'exercise_id', 'exercise_name', 'exercise_movement_type',
        'exercise_bodysplit', 'created_at', 'updated_at'
    ]]

    inserted_rows = 0
    updated_rows = 0

    try:
        with engine.begin() as conn:
            for row in exercise_df.itertuples(index=False):
                # Check if exercise exists
                exists_stmt = select(exercises).where(exercises.c.exercise_id == str(row.exercise_id))
                result = conn.execute(exists_stmt).fetchone()

                if result is None:
                    ins_stmt = insert(exercises).values(
                        exercise_id=row.exercise_id,
                        exercise_name=row.exercise_name,
                        exercise_movement_type=row.exercise_movement_type,
                        exercise_bodysplit=row.exercise_bodysplit,
                        created_at=row.created_at,
                        updated_at=row.updated_at
                    )
                    conn.execute(ins_stmt)
                    inserted_rows += 1
                else:
                    needs_update = (
                        result.exercise_name != row.exercise_name or
                        result.exercise_movement_type != row.exercise_movement_type or
                        result.exercise_bodysplit != row.exercise_bodysplit
                    )
                    if needs_update:
                        upd_stmt = (
                            update(exercises)
                            .where(exercises.c.exercise_id == str(row.exercise_id))
                            .values(
                                exercise_name=row.exercise_name,
                                exercise_movement_type=row.exercise_movement_type,
                                exercise_bodysplit=row.exercise_bodysplit,
                                updated_at=pd.Timestamp.now()
                            )
                        )
                        conn.execute(upd_stmt)
                        updated_rows += 1

        success_text = (
            f"{inserted_rows} inserted rows have been loaded into *staging_layer.exercises*\n"
            f"{updated_rows} rows have been updated in *staging_layer.exercises*"
        )
        success_msg.append(success_text)
        logger.info(success_text)

    except Exception as e:
        error_text = f"[extract_exercises] Error {e} occurred. Could not insert/update into exercises"
        error_msg.append(error_text)
        logger.exception(error_text)
