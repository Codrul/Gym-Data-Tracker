CREATE OR REPLACE PROCEDURE load_resistances_to_cl()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_inserted INT := 0;
    v_rows_updated  INT := 0;
    v_message_text  TEXT;
    v_detail_text   TEXT;
    v_hint_text     TEXT;
    v_status_code   INT;
BEGIN
    BEGIN
        UPDATE cleansing_layer.cl_resistance_types tgt
        SET
            resistance_type     = COALESCE(NULLIF(TRIM(src.resistance_type), ''), tgt.resistance_type),
            resistance_category = COALESCE(NULLIF(TRIM(src.resistance_category), ''), tgt.resistance_category),
            updated_at          = now()
        FROM staging_layer.resistance_types src
        WHERE tgt.resistance_src_id = src.resistance_id
          AND (
                (src.resistance_type IS NOT NULL AND TRIM(src.resistance_type) <> ''
                 AND regexp_replace(
                         upper(substr(TRIM(src.resistance_type),1,1)) || lower(substr(TRIM(src.resistance_type),2)),
                         '\s+', ' ', 'g'
                     ) IS DISTINCT FROM
                     regexp_replace(
                         upper(substr(TRIM(tgt.resistance_type),1,1)) || lower(substr(TRIM(tgt.resistance_type),2)),
                         '\s+', ' ', 'g'
                     )
                )
                OR
                (src.resistance_category IS NOT NULL AND TRIM(src.resistance_category) <> ''
                 AND regexp_replace(
                         upper(substr(TRIM(src.resistance_category),1,1)) || lower(substr(TRIM(src.resistance_category),2)),
                         '\s+', ' ', 'g'
                     ) IS DISTINCT FROM
                     regexp_replace(
                         upper(substr(TRIM(tgt.resistance_category),1,1)) || lower(substr(TRIM(tgt.resistance_category),2)),
                         '\s+', ' ', 'g'
                     )
                )
          );

        GET DIAGNOSTICS v_rows_updated = ROW_COUNT;

        WITH cleansed AS (
            SELECT DISTINCT
                resistance_id,
                COALESCE(NULLIF(TRIM(resistance_type), ''), 'N/A') AS resistance_type,
                COALESCE(NULLIF(TRIM(resistance_category), ''), 'N/A') AS resistance_category
            FROM staging_layer.resistance_types
        )
        INSERT INTO cleansing_layer.cl_resistance_types(
            resistance_id,
            resistance_src_id,
            resistance_type,
            resistance_category,
            created_at
        )
        SELECT
            nextval('cleansing_layer.resistance_id_seq'),
            c.resistance_id,
            COALESCE(NULLIF(c.resistance_type, ''), 'N/A'),
            COALESCE(NULLIF(c.resistance_category, ''), 'N/A'),
            now()
        FROM cleansed c
        WHERE NOT EXISTS (
            SELECT 1
            FROM cleansing_layer.cl_resistance_types tgt
            WHERE tgt.resistance_src_id = c.resistance_id
              AND COALESCE(
                      regexp_replace(
                          upper(substr(TRIM(tgt.resistance_type),1,1)) || lower(substr(TRIM(tgt.resistance_type),2)),
                          '\s+', ' ', 'g'
                      ), 'N/A'
                  ) = COALESCE(
                      regexp_replace(
                          upper(substr(TRIM(c.resistance_type),1,1)) || lower(substr(TRIM(c.resistance_type),2)),
                          '\s+', ' ', 'g'
                      ), 'N/A'
                  )
              AND COALESCE(
                      regexp_replace(
                          upper(substr(TRIM(tgt.resistance_category),1,1)) || lower(substr(TRIM(tgt.resistance_category),2)),
                          '\s+', ' ', 'g'
                      ), 'N/A'
                  ) = COALESCE(
                      regexp_replace(
                          upper(substr(TRIM(c.resistance_category),1,1)) || lower(substr(TRIM(c.resistance_category),2)),
                          '\s+', ' ', 'g'
                      ), 'N/A'
                  )
        );

        GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

        RAISE NOTICE '% rows inserted and % rows updated into cleansing_layer.cl_resistance_types', v_rows_inserted, v_rows_updated;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_message_text = MESSAGE_TEXT,
                v_detail_text  = PG_EXCEPTION_DETAIL,
                v_hint_text    = PG_EXCEPTION_HINT;
            v_status_code := 2;
            RAISE EXCEPTION 'Error occurred: %, Details: %, Hint: %', v_message_text, v_detail_text, v_hint_text;
    END;
END;
$$;

CALL load_resistances_to_cl();
