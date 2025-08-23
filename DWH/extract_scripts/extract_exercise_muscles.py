import pandas as pd
import logging
from sqlalchemy import MetaData, Table, Column, select, insert, update, String, DateTime

metadata = MetaData()

exercise_muscle = Table(
    'exercise_muscle', metadata,
    Column('exercise_id', String, primary_key=True),
    Column('exercise_name', String),
    Column('muscle_id', String, primary_key=True),
    Column('muscle_name', String),
    Column('muscle_role', String),
    Column('created_at', DateTime),
    Column('updated_at', DateTime),
    schema='staging_layer'
)

# log my stuff so i see it in the ui 
logger = logging.getLogger("airflow.task")


def load_exercise_muscle(gc, engine, success_msg, error_msg):
    """
    Extract exercise-muscle mapping from Google Sheets and load/update into staging_layer.exercise_muscle.
    """
    try:
        logger.info("[extract_exercise_muscle] Connecting to Google Sheets...")
        spreadsheet = gc.open_by_key('1OiufKuY1WB_-tzfvKWZh9OHeCCEX81jQ1KHuNE5lZsQ')
        worksheet = spreadsheet.worksheet('Exercise_Muscle')
        exercise_muscle_table = worksheet.get_all_records()
        logger.info("[extract_exercise_muscle] Retrieved %s rows from Google Sheets", len(exercise_muscle_table))
    except Exception as e:
        error_text = f"[extract_exercise_muscle] Error {e} occurred. Failed to load from Google Sheets"
        error_msg.append(error_text)
        logger.exception(error_text)
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
    df['created_at'] = pd.Timestamp.now()
    df['updated_at'] = pd.Timestamp.now()

    exercise_muscle_df = df[['exercise_id', 'exercise_name',
                             'muscle_id', 'muscle_name', 'muscle_role', 'created_at', 'updated_at']]

    inserted_rows = 0
    updated_rows = 0

    try:
        with engine.begin() as conn:
            for row in exercise_muscle_df.itertuples(index=False):
                exists_stmt = select(
                    exercise_muscle.c.exercise_name,
                    exercise_muscle.c.muscle_name,
                    exercise_muscle.c.muscle_role
                ).where(
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
                        muscle_role=row.muscle_role,
                        created_at=row.created_at,
                        updated_at=row.updated_at
                    )
                    conn.execute(ins_stmt)
                    inserted_rows += 1
                else:
                    existing_exercise_name = str(result[0]).strip().lower()
                    existing_muscle_name = str(result[1]).strip().lower()
                    existing_muscle_role = str(result[2]).strip().lower()
                    needs_update = (
                        existing_exercise_name != str(row.exercise_name).strip().lower() or
                        existing_muscle_name != str(row.muscle_name).strip().lower() or
                        existing_muscle_role != str(row.muscle_role).strip().lower()
                    )
                    if needs_update:
                        upd_stmt = (
                            update(exercise_muscle)
                            .where(
                                (exercise_muscle.c.exercise_id == str(row.exercise_id)) &
                                (exercise_muscle.c.muscle_id == str(row.muscle_id))
                            )
                            .values(
                                exercise_name=row.exercise_name,
                                muscle_name=row.muscle_name,
                                muscle_role=row.muscle_role,
                                updated_at=pd.Timestamp.now()
                            )
                        )
                        conn.execute(upd_stmt)
                        updated_rows += 1

        success_text = (
            f"{inserted_rows} rows have been inserted into *staging_layer.exercise_muscle*\n"
            f"{updated_rows} rows have been updated in *staging_layer.exercise_muscle*"
        )
        success_msg.append(success_text)
        logger.info(success_text)

    except Exception as e:
        error_text = f"[extract_exercise_muscle] Error {e} occurred. Could not insert/update into exercise_muscle"
        error_msg.append(error_text)
        logger.exception(error_text)
