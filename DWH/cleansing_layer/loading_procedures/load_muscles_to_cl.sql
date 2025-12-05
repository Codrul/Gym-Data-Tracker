CREATE OR REPLACE PROCEDURE load_muscles_to_cl()
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
        UPDATE cleansing_layer.cl_muscles tgt
        SET
            muscle_name  = COALESCE(NULLIF(TRIM(src.muscle_name), ''), tgt.muscle_name),
            muscle_group = COALESCE(NULLIF(TRIM(src.muscle_group), ''), tgt.muscle_group),
            updated_at   = now()
        FROM staging_layer.muscles src
        WHERE tgt.muscle_src_id = src.muscle_id
          AND (
                (src.muscle_name IS NOT NULL AND TRIM(src.muscle_name) <> ''
                 AND regexp_replace(
                         upper(substr(TRIM(src.muscle_name),1,1)) || lower(substr(TRIM(src.muscle_name),2)),
                         '\s+', ' ', 'g'
                     ) IS DISTINCT FROM
                     regexp_replace(
                         upper(substr(TRIM(tgt.muscle_name),1,1)) || lower(substr(TRIM(tgt.muscle_name),2)),
                         '\s+', ' ', 'g'
                     )
                )
                OR
                (src.muscle_group IS NOT NULL AND TRIM(src.muscle_group) <> ''
                 AND regexp_replace(
                         upper(substr(TRIM(src.muscle_group),1,1)) || lower(substr(TRIM(src.muscle_group),2)),
                         '\s+', ' ', 'g'
                     ) IS DISTINCT FROM
                     regexp_replace(
                         upper(substr(TRIM(tgt.muscle_group),1,1)) || lower(substr(TRIM(tgt.muscle_group),2)),
                         '\s+', ' ', 'g'
                     )
                )
          );

        GET DIAGNOSTICS v_rows_updated = ROW_COUNT;

        WITH cleansed AS (
            SELECT DISTINCT
                muscle_id,
                COALESCE(NULLIF(TRIM(muscle_name), ''), 'N/A') AS muscle_name,
                COALESCE(NULLIF(TRIM(muscle_group), ''), 'N/A') AS muscle_group
            FROM staging_layer.muscles
        )
        INSERT INTO cleansing_layer.cl_muscles(
            muscle_id,
            muscle_src_id,
            muscle_name,
            muscle_group,
            created_at
        )
        SELECT
            nextval('cleansing_layer.muscle_id_seq'),
            c.muscle_id,
            COALESCE(NULLIF(c.muscle_name, ''), 'N/A'),
            COALESCE(NULLIF(c.muscle_group, ''), 'N/A'),
            now()
        FROM cleansed c
        WHERE NOT EXISTS (
            SELECT 1
            FROM cleansing_layer.cl_muscles tgt
            WHERE tgt.muscle_src_id = c.muscle_id
              AND COALESCE(
                      regexp_replace(
                          upper(substr(TRIM(tgt.muscle_name),1,1)) || lower(substr(TRIM(tgt.muscle_name),2)),
                          '\s+', ' ', 'g'
                      ), 'N/A'
                  ) = COALESCE(
                      regexp_replace(
                          upper(substr(TRIM(c.muscle_name),1,1)) || lower(substr(TRIM(c.muscle_name),2)),
                          '\s+', ' ', 'g'
                      ), 'N/A'
                  )
              AND COALESCE(
                      regexp_replace(
                          upper(substr(TRIM(tgt.muscle_group),1,1)) || lower(substr(TRIM(tgt.muscle_group),2)),
                          '\s+', ' ', 'g'
                      ), 'N/A'
                  ) = COALESCE(
                      regexp_replace(
                          upper(substr(TRIM(c.muscle_group),1,1)) || lower(substr(TRIM(c.muscle_group),2)),
                          '\s+', ' ', 'g'
                      ), 'N/A'
                  )
        );

        GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

        RAISE NOTICE '% rows inserted and % rows updated into cleansing_layer.cl_muscles', v_rows_inserted, v_rows_updated;

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

CALL load_muscles_to_cl();
