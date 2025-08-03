import pandas as pd
from sqlalchemy import (
    MetaData, Table, Column,
    select, insert, String, DateTime
)
metadata = MetaData()

workouts = Table(
    'workouts', metadata,
    Column('workout_id', String, primary_key=True),
    Column('date', String),
    Column('set_number', String),
    Column('exercise', String),
    Column('reps', String),
    Column('load', String),
    Column('resistance_type', String),
    Column('set_type', String),
    Column('comments', String),
    Column('workout_type', String),
    Column('created_at', DateTime),
    schema='staging_layer'
)


def load_workouts(gc, engine, success_msg, error_msg):
    try:
        spreadsheet = gc.open_by_key(
            '1OiufKuY1WB_-tzfvKWZh9OHeCCEX81jQ1KHuNE5lZsQ'
        )
        worksheet = spreadsheet.worksheet('Workouts')
        workouts_table = worksheet.get_all_records()
    except Exception as e:
        error_msg.append(f'[extract_workouts] Error {e} occurred. Failed to load from Google Sheets')
        return

    df = pd.DataFrame(workouts_table)
    column_mapping = {
        'Workout number': 'workout_id',
        'Date': 'date',
        'Set': 'set_number',
        'Exercise': 'exercise',
        'Reps': 'reps',
        'Load': 'load',
        'Resistance type': 'resistance_type',
        'Set type': 'set_type',
        'Comments': 'comments',
        'Workout type': 'workout_type'
    }
    df.rename(columns=column_mapping, inplace=True)
    df['created_at'] = pd.Timestamp.now()
    df = df[['workout_id', 'date', 'set_number', 'exercise', 'reps', 'load', 'resistance_type', 'set_type',
             'comments', 'workout_type', 'created_at']]
    try:
        inserted_rows = 0
        with engine.connect() as conn:
            for row in df.itertuples(index=False):
                exists_stmt = select(workouts.c.workout_id).where(
                    (workouts.c.workout_id == str(row.workout_id)) &
                    (workouts.c.date == str(row.date)) &
                    (workouts.c.set_number == str(row.set_number)) &
                    (workouts.c.exercise == str(row.exercise))
                )
                result = conn.execute(exists_stmt).fetchone()
                if result is None:
                    ins_stmt = insert(workouts).values(
                        workout_id=row.workout_id,
                        date=row.date,
                        set_number=row.set_number,
                        exercise=row.exercise,
                        reps=row.reps,
                        load=row.load,
                        resistance_type=row.resistance_type,
                        set_type=row.set_type,
                        comments=row.comments,
                        workout_type=row.workout_type,
                        created_at=row.created_at
                    )
                    conn.execute(ins_stmt)
                    inserted_rows += 1
                conn.commit()
        success_msg.append(f"{inserted_rows} rows have been loaded into *staging_layer.workouts*")
        return
    except Exception as e:
        error_msg.append(f'Error {e} occurred. Could not insert into workouts')
