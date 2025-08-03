import pandas as pd
from sqlalchemy import (
    MetaData, Table, Column,
    select, insert, String, DateTime
)

metadata = MetaData()
# self explaining module name really
muscles = Table(
    'muscles', metadata,
    Column('muscle_id', String, primary_key=True),
    Column('muscle_name', String),
    Column('muscle_group', String),
    Column('created_at', DateTime),
    schema='staging_layer'
)


def load_muscles(gc, engine, success_msg, error_msg):
    try:
        spreadsheet = gc.open_by_key(
            '1OiufKuY1WB_-tzfvKWZh9OHeCCEX81jQ1KHuNE5lZsQ'
        )
        worksheet = spreadsheet.worksheet('Muscles')
        muscles_table = worksheet.get_all_records()
    except Exception as e:
        error_msg.append(f'Error {e} occurred. Failed to load from Google Sheets')
        return

    df = pd.DataFrame(muscles_table)
    column_mapping = {
        'ID': 'muscle_id',
        'Muscle name': 'muscle_name',
        'Muscle groups': 'muscle_group'
    }
    df.rename(columns=column_mapping, inplace=True)
    df['created_at'] = pd.Timestamp.now()

    df = df[
        ['muscle_id', 'muscle_name', 'muscle_group', 'created_at']
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
                        created_at=row.created_at
                    )
                    conn.execute(ins_stmt)
                    inserted_rows += 1
            conn.commit()
            success_msg.append(f'[extract_muscle] has inserted {inserted_rows} rows into staging_layer.muscles successfully')
        return
    except Exception as e:
        error_msg.append(f'Error {e} occurred. Could not insert into muscle')
