CREATE OR REPLACE PROCEDURE load_exercise_muscle_to_cl()
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
        -- Update existing rows with cleaned values
        UPDATE cleansing_layer.cl_exercise_muscle tgt
        SET
            exercise_name = COALESCE(NULLIF(TRIM(em.exercise_name), ''), tgt.exercise_name),
            muscle_name   = COALESCE(NULLIF(TRIM(em.muscle_name), ''), tgt.muscle_name),
            muscle_role   = COALESCE(NULLIF(TRIM(em.muscle_role), ''), tgt.muscle_role),
            updated_at    = now()
        FROM staging_layer.exercise_muscle em
        JOIN cleansing_layer.cl_exercises e ON em.exercise_id = e.exercise_src_id
        JOIN cleansing_layer.cl_muscles m ON em.muscle_id = m.muscle_src_id
        WHERE tgt.exercise_src_id = e.exercise_src_id
          AND tgt.muscle_src_id = m.muscle_src_id
          AND (
                COALESCE(
                    regexp_replace(
                        upper(substr(TRIM(em.exercise_name), 1, 1)) || lower(substr(TRIM(em.exercise_name), 2)),
                        '\s+', ' ', 'g'
                    ), 'N/A'
                ) IS DISTINCT FROM 
                COALESCE(
                    regexp_replace(
                        upper(substr(TRIM(tgt.exercise_name), 1, 1)) || lower(substr(TRIM(tgt.exercise_name), 2)),
                        '\s+', ' ', 'g'
                    ), 'N/A'
                )
                OR
                COALESCE(
                    regexp_replace(
                        upper(substr(TRIM(em.muscle_name), 1, 1)) || lower(substr(TRIM(em.muscle_name), 2)),
                        '\s+', ' ', 'g'
                    ), 'N/A'
                ) IS DISTINCT FROM 
                COALESCE(
                    regexp_replace(
                        upper(substr(TRIM(tgt.muscle_name), 1, 1)) || lower(substr(TRIM(tgt.muscle_name), 2)),
                        '\s+', ' ', 'g'
                    ), 'N/A'
                )
                OR
                COALESCE(
                    regexp_replace(
                        upper(substr(TRIM(em.muscle_role), 1, 1)) || lower(substr(TRIM(em.muscle_role), 2)),
                        '\s+', ' ', 'g'
                    ), 'N/A'
                ) IS DISTINCT FROM 
                COALESCE(
                    regexp_replace(
                        upper(substr(TRIM(tgt.muscle_role), 1, 1)) || lower(substr(TRIM(tgt.muscle_role), 2)),
                        '\s+', ' ', 'g'
                    ), 'N/A'
                )
          );

        GET DIAGNOSTICS v_rows_updated = ROW_COUNT;

        -- Insert new rows with cleaned values
        WITH cleansed AS (
            SELECT DISTINCT
                em.exercise_id,
                em.exercise_id AS exercise_src_id,
                COALESCE(NULLIF(TRIM(em.exercise_name), ''), 'N/A') AS exercise_name,
                em.muscle_id,
                em.muscle_id AS muscle_src_id,
                COALESCE(NULLIF(TRIM(em.muscle_name), ''), 'N/A') AS muscle_name,
                COALESCE(NULLIF(TRIM(em.muscle_role), ''), 'N/A') AS muscle_role
            FROM staging_layer.exercise_muscle em
        )
        INSERT INTO cleansing_layer.cl_exercise_muscle(
            exercise_id,
            exercise_src_id,
            exercise_name,
            muscle_id,
            muscle_src_id,
            muscle_name,
            muscle_role,
            created_at
        )
        SELECT
            e.exercise_id,
            c.exercise_src_id,
            regexp_replace(
                upper(substr(c.exercise_name, 1, 1)) || lower(substr(c.exercise_name, 2)),
                '\s+', ' ', 'g'
            ),
            m.muscle_id,
            c.muscle_src_id,
            regexp_replace(
                upper(substr(c.muscle_name, 1, 1)) || lower(substr(c.muscle_name, 2)),
                '\s+', ' ', 'g'
            ),
            regexp_replace(
                upper(substr(c.muscle_role, 1, 1)) || lower(substr(c.muscle_role, 2)),
                '\s+', ' ', 'g'
            ),
            now()
        FROM cleansed c
        JOIN cleansing_layer.cl_exercises e ON c.exercise_src_id = e.exercise_src_id
        JOIN cleansing_layer.cl_muscles m ON c.muscle_src_id = m.muscle_src_id
        WHERE NOT EXISTS (
            SELECT 1
            FROM cleansing_layer.cl_exercise_muscle tgt
            WHERE tgt.exercise_src_id = e.exercise_src_id
              AND tgt.muscle_src_id = m.muscle_src_id
        );

        GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

        RAISE NOTICE '% rows were inserted and % updated into cleansing_layer.cl_exercise_muscle', v_rows_affected, v_rows_updated;

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

CALL load_exercise_muscle_to_cl()
