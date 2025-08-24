import pandas as pd
import logging
from sqlalchemy import MetaData, Table, Column, select, insert, update, String, DateTime

metadata = MetaData()

muscles = Table(
    'muscles', metadata,
    Column('muscle_id', String, primary_key=True),
    Column('muscle_name', String),
    Column('muscle_group', String),
    Column('created_at', DateTime),
    Column('updated_at', DateTime),
    schema='staging_layer'
)

# so I can see stuff in airflow UI 
logger = logging.getLogger("airflow.task")


def load_muscles(gc, engine, success_msg, error_msg):
    """
    Extract muscles from Google Sheets and load into staging_layer.muscles.
    """
    try:
        logger.info("[extract_muscles] Connecting to Google Sheets...")
        spreadsheet = gc.open_by_key('1OiufKuY1WB_-tzfvKWZh9OHeCCEX81jQ1KHuNE5lZsQ')
        worksheet = spreadsheet.worksheet('Muscles')
        muscles_table = worksheet.get_all_records()
        logger.info("[extract_muscles] Retrieved %s rows from Google Sheets", len(muscles_table))
    except Exception as e:
        error_text = f"[extract_muscles] Error {e} occurred. Failed to load from Google Sheets"
        error_msg.append(error_text)
        logger.exception(error_text)
        return

    df = pd.DataFrame(muscles_table)
    column_mapping = {
        'ID': 'muscle_id',
        'Muscle name': 'muscle_name',
        'Muscle groups': 'muscle_group'
    }
    df.rename(columns=column_mapping, inplace=True)
    df['created_at'] = pd.Timestamp.now()
    df['updated_at'] = pd.Timestamp.now()

    muscles_df = df[['muscle_id', 'muscle_name', 'muscle_group', 'created_at', 'updated_at']]

    inserted_rows = 0
    updated_rows = 0

    try:
        with engine.begin() as conn:
            for row in muscles_df.itertuples(index=False):
                exists_stmt = select(muscles).where(muscles.c.muscle_id == str(row.muscle_id))
                result = conn.execute(exists_stmt).fetchone()

                if result is None:
                    ins_stmt = insert(muscles).values(
                        muscle_id=row.muscle_id,
                        muscle_name=row.muscle_name,
                        muscle_group=row.muscle_group,
                        created_at=row.created_at,
                        updated_at=row.updated_at
                    )
                    conn.execute(ins_stmt)
                    inserted_rows += 1
                else:
                    needs_update = (
                        str(result.muscle_name).strip().lower() != str(row.muscle_name).strip().lower() or
                        str(result.muscle_group).strip().lower() != str(row.muscle_group).strip().lower()
                    )
                    if needs_update:
                        upd_stmt = (
                            update(muscles)
                            .where(muscles.c.muscle_id == str(row.muscle_id))
                            .values(
                                muscle_name=row.muscle_name,
                                muscle_group=row.muscle_group,
                                updated_at=pd.Timestamp.now()
                            )
                        )
                        conn.execute(upd_stmt)
                        updated_rows += 1

        success_text = (
            f"{inserted_rows} rows inserted into *staging_layer.muscles*\n"
            f"{updated_rows} rows updated in *staging_layer.muscles*"
        )
        success_msg.append(success_text)
        logger.info(success_text)

    except Exception as e:
        error_text = f"[extract_muscles] Error {e} occurred. Could not insert/update into muscles"
        error_msg.append(error_text)
        logger.exception(error_text)
