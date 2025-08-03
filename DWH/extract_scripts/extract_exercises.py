import pandas as pd
from sqlalchemy import (
    MetaData, Table, Column,
    select, insert, String, DateTime
)

metadata = MetaData()
# define the table
exercises = Table(
    'exercises', metadata,
    Column('exercise_id', String, primary_key=True),
    Column('exercise_name', String),
    Column('exercise_movement_type', String),
    Column('exercise_bodysplit', String),
    Column('created_at', DateTime),
    schema='staging_layer'
)


def load_exercises(gc, engine, error_msg, success_msg):
    try:
        spreadsheet = gc.open_by_key(
            '1OiufKuY1WB_-tzfvKWZh9OHeCCEX81jQ1KHuNE5lZsQ'
        )
        worksheet = spreadsheet.worksheet('Exercises')
        exercise_table = worksheet.get_all_records()
    except Exception as e:
        error_msg.append(f'Error {e} occurred. Failed to load from Google Sheets')
        return

    df = pd.DataFrame(exercise_table)
    column_mapping = {
        'ID': 'exercise_id',
        'Name': 'exercise_name',
        'Movement type': 'exercise_movement_type',
        'Upper/Lower': 'exercise_bodysplit'
    }
    df.rename(columns=column_mapping, inplace=True)
    df['created_at'] = pd.Timestamp.now()

    exercise_df = df[
        ['exercise_id', 'exercise_name',
         'exercise_movement_type', 'exercise_bodysplit', 'created_at']
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
                        exercise_bodysplit=row.exercise_bodysplit,
                        created_at=row.created_at
                    )
                    conn.execute(ins_stmt)
                    inserted_rows += 1
            conn.commit()
            success_msg.append(f"{inserted_rows} rows have been loaded into *staging_layer.exercises*")
        return
    except Exception as e:
        error_msg.append(f'Error {e} occurred. Could not insert into *staging_layer.exercises*')
