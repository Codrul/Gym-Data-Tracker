resistance_types = Table(
    'resistance_types', metadata,
    Column('resistance_id', String, primary_key=True),
    Column('resistance_type', String),
    Column('resistance_category', String),
    schema='staging_layer'
)

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


