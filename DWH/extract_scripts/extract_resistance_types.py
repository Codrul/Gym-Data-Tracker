import pandas as pd
import logging
from sqlalchemy import MetaData, Table, Column, select, insert, update, String, DateTime

metadata = MetaData()

resistance_types = Table(
    'resistance_types', metadata,
    Column('resistance_id', String, primary_key=True),
    Column('resistance_type', String),
    Column('resistance_category', String),
    Column('created_at', DateTime),
    Column('updated_at', DateTime),
    schema='staging_layer'
)

logger = logging.getLogger("airflow.task")


def load_resistance_types(gc, engine, success_msg, error_msg):
    """
    Extract resistance types from Google Sheets and load into staging_layer.resistance_types.
    """
    try:
        logger.info("[extract_resistance_types] Connecting to Google Sheets...")
        spreadsheet = gc.open_by_key('1OiufKuY1WB_-tzfvKWZh9OHeCCEX81jQ1KHuNE5lZsQ')
        worksheet = spreadsheet.worksheet('Resistance_types')
        resistance_table = worksheet.get_all_records()
        logger.info("[extract_resistance_types] Retrieved %s rows from Google Sheets", len(resistance_table))
    except Exception as e:
        error_text = f"[extract_resistance_types] Error {e} occurred. Failed to load from Google Sheets"
        error_msg.append(error_text)
        logger.exception(error_text)
        return

    df = pd.DataFrame(resistance_table)
    column_mapping = {
        'Resistance_ID': 'resistance_id',
        'Resistance': 'resistance_type',
        'Resistance_category': 'resistance_category',
    }
    df.rename(columns=column_mapping, inplace=True)
    df['created_at'] = pd.Timestamp.now()
    df['updated_at'] = pd.Timestamp.now()

    resistance_df = df[['resistance_id', 'resistance_type', 'resistance_category', 'created_at', 'updated_at']]

    inserted_rows = 0
    updated_rows = 0

    try:
        with engine.begin() as conn:
            for row in resistance_df.itertuples(index=False):
                exists_stmt = select(resistance_types).where(resistance_types.c.resistance_id == str(row.resistance_id))
                result = conn.execute(exists_stmt).fetchone()

                if result is None:
                    ins_stmt = insert(resistance_types).values(
                        resistance_id=row.resistance_id,
                        resistance_type=row.resistance_type,
                        resistance_category=row.resistance_category,
                        created_at=row.created_at,
                        updated_at=row.updated_at
                    )
                    conn.execute(ins_stmt)
                    inserted_rows += 1
                else:
                    needs_update = (
                        result.resistance_type != row.resistance_type or
                        result.resistance_category != row.resistance_category
                    )
                    if needs_update:
                        upd_stmt = (
                            update(resistance_types)
                            .where(resistance_types.c.resistance_id == str(row.resistance_id))
                            .values(
                                resistance_type=row.resistance_type,
                                resistance_category=row.resistance_category,
                                updated_at=pd.Timestamp.now()
                            )
                        )
                        conn.execute(upd_stmt)
                        updated_rows += 1

        success_text = (
            f"{inserted_rows} rows inserted into *staging_layer.resistance_types*\n"
            f"{updated_rows} rows updated in *staging_layer.resistance_types*"
        )
        success_msg.append(success_text)
        logger.info(success_text)

    except Exception as e:
        error_text = f"[extract_resistance_types] Error {e} occurred. Could not insert/update into resistance_types"
        error_msg.append(error_text)
        logger.exception(error_text)
