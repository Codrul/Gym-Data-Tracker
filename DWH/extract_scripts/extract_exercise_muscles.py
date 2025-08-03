import pandas as pd
from sqlalchemy import (
    MetaData, Table, Column,
    select, insert, String, DateTime
)

metadata = MetaData()
exercise_muscle = Table(
    'exercise_muscle', metadata,
    Column('exercise_id', String, primary_key=True),
    Column('exercise_name', String),
    Column('muscle_id', String, primary_key=True),
    Column('muscle_name', String),
    Column('muscle_role', String),
    Column('created_at', DateTime),
    schema='staging_layer'
)


def load_exercise_muscle(gc, engine, error_msg, success_msg):
    try:
        spreadsheet = gc.open_by_key(
            '1OiufKuY1WB_-tzfvKWZh9OHeCCEX81jQ1KHuNE5lZsQ'
        )
        worksheet = spreadsheet.worksheet('Exercise_Muscle')
        exercise_muscle_table = worksheet.get_all_records()
    except Exception as e:
        error_msg.append(f'Error {e} occurred. Failed to load from Google Sheets')
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

    df = df[
        ['exercise_id', 'exercise_name',
         'muscle_id', 'muscle_name', 'muscle_role', 'created_at']
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
                        muscle_role=row.muscle_role,
                        created_at=row.created_at
                    )
                    conn.execute(ins_stmt)
                    inserted_rows += 1
            conn.commit()
            success_msg.append(f"{inserted_rows} rows have been loaded into *staging_layer.exercise_muscle*")
        return
    except Exception as e:
        error_msg.append(f'Error {e} occurred. Could not insert into exercise_muscle')
        return
