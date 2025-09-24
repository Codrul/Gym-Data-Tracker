CREATE OR REPLACE PROCEDURE load_exercises_to_cl()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INT := 0;
    v_rows_updated  INT := 0;
    v_message_text  TEXT;
    v_detail_text   TEXT;
    v_hint_text     TEXT;
    v_status_code   INT;
BEGIN
    BEGIN

        UPDATE cleansing_layer.cl_exercises tgt
        SET
            exercise_name          = COALESCE(NULLIF(TRIM(em.exercise_name), ''), tgt.exercise_name),
            exercise_movement_type = COALESCE(NULLIF(TRIM(em.exercise_movement_type), ''), tgt.exercise_movement_type),
            exercise_bodysplit     = COALESCE(NULLIF(TRIM(em.exercise_bodysplit), ''), tgt.exercise_bodysplit),
            updated_at             = now()
        FROM staging_layer.exercises em
        WHERE tgt.exercise_src_id = em.exercise_id
          AND (
                -- Update only if normalized staging value differs from normalized target
                (em.exercise_name IS NOT NULL AND TRIM(em.exercise_name) <> ''
                 AND regexp_replace(
                         upper(substr(TRIM(em.exercise_name),1,1)) || lower(substr(TRIM(em.exercise_name),2)),
                         '\s+', ' ', 'g'
                     ) IS DISTINCT FROM
                     regexp_replace(
                         upper(substr(TRIM(tgt.exercise_name),1,1)) || lower(substr(TRIM(tgt.exercise_name),2)),
                         '\s+', ' ', 'g'
                     )
                )
                OR
                (em.exercise_movement_type IS NOT NULL AND TRIM(em.exercise_movement_type) <> ''
                 AND regexp_replace(
                         upper(substr(TRIM(em.exercise_movement_type),1,1)) || lower(substr(TRIM(em.exercise_movement_type),2)),
                         '\s+', ' ', 'g'
                     ) IS DISTINCT FROM
                     regexp_replace(
                         upper(substr(TRIM(tgt.exercise_movement_type),1,1)) || lower(substr(TRIM(tgt.exercise_movement_type),2)),
                         '\s+', ' ', 'g'
                     )
                )
                OR
                (em.exercise_bodysplit IS NOT NULL AND TRIM(em.exercise_bodysplit) <> ''
                 AND regexp_replace(
                         upper(substr(TRIM(em.exercise_bodysplit),1,1)) || lower(substr(TRIM(em.exercise_bodysplit),2)),
                         '\s+', ' ', 'g'
                     ) IS DISTINCT FROM
                     regexp_replace(
                         upper(substr(TRIM(tgt.exercise_bodysplit),1,1)) || lower(substr(TRIM(tgt.exercise_bodysplit),2)),
                         '\s+', ' ', 'g'
                     )
                )
          );

        GET DIAGNOSTICS v_rows_updated = ROW_COUNT;


        WITH cleansed AS (
            SELECT DISTINCT
                exercise_id,
                COALESCE(NULLIF(TRIM(exercise_name), ''), 'N/A') AS exercise_name,
                COALESCE(NULLIF(TRIM(exercise_movement_type), ''), 'N/A') AS exercise_movement_type,
                COALESCE(NULLIF(TRIM(exercise_bodysplit), ''), 'N/A') AS exercise_bodysplit
            FROM staging_layer.exercises
        )
        INSERT INTO cleansing_layer.cl_exercises(
            exercise_id,
            exercise_src_id,
            exercise_name,
            exercise_movement_type,
            exercise_bodysplit,
            created_at
        )
        SELECT
            nextval('cleansing_layer.exercise_id_seq'),
            c.exercise_id,
            COALESCE(NULLIF(c.exercise_name, ''), 'N/A'),
            COALESCE(NULLIF(c.exercise_movement_type, ''), 'N/A'),
            COALESCE(NULLIF(c.exercise_bodysplit, ''), 'N/A'),
            now()
        FROM cleansed c
        WHERE NOT EXISTS (
            SELECT 1
            FROM cleansing_layer.cl_exercises tgt
            WHERE tgt.exercise_src_id = c.exercise_id
              AND COALESCE(
                      regexp_replace(
                          upper(substr(TRIM(tgt.exercise_name),1,1)) || lower(substr(TRIM(tgt.exercise_name),2)),
                          '\s+', ' ', 'g'
                      ), 'N/A'
                  ) = COALESCE(
                      regexp_replace(
                          upper(substr(TRIM(c.exercise_name),1,1)) || lower(substr(TRIM(c.exercise_name),2)),
                          '\s+', ' ', 'g'
                      ), 'N/A'
                  )
              AND COALESCE(
                      regexp_replace(
                          upper(substr(TRIM(tgt.exercise_movement_type),1,1)) || lower(substr(TRIM(tgt.exercise_movement_type),2)),
                          '\s+', ' ', 'g'
                      ), 'N/A'
                  ) = COALESCE(
                      regexp_replace(
                          upper(substr(TRIM(c.exercise_movement_type),1,1)) || lower(substr(TRIM(c.exercise_movement_type),2)),
                          '\s+', ' ', 'g'
                      ), 'N/A'
                  )
              AND COALESCE(
                      regexp_replace(
                          upper(substr(TRIM(tgt.exercise_bodysplit),1,1)) || lower(substr(TRIM(tgt.exercise_bodysplit),2)),
                          '\s+', ' ', 'g'
                      ), 'N/A'
                  ) = COALESCE(
                      regexp_replace(
                          upper(substr(TRIM(c.exercise_bodysplit),1,1)) || lower(substr(TRIM(c.exercise_bodysplit),2)),
                          '\s+', ' ', 'g'
                      ), 'N/A'
                  )
        );

        GET DIAGNOSTICS v_rows_affected = ROW_COUNT;


        RAISE NOTICE '% rows were inserted and % updated into cleansing_layer.cl_exercises',
                     v_rows_affected, v_rows_updated;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_message_text = MESSAGE_TEXT,
                v_detail_text  = PG_EXCEPTION_DETAIL,
                v_hint_text    = PG_EXCEPTION_HINT;
            v_status_code := 2;
            RAISE EXCEPTION 'Error occurred: %, Details: %, Hint: %',
                            v_message_text, v_detail_text, v_hint_text;
    END;
END;
$$;

CALL load_exercises_to_cl();
