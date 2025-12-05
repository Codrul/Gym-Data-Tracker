CREATE OR REPLACE PROCEDURE load_workouts_to_cl()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INT := 0;
    v_message_text  TEXT;
    v_detail_text   TEXT;
    v_hint_text     TEXT;
    v_status_code   INT;
BEGIN
    BEGIN
        WITH cleansed AS (
            SELECT DISTINCT
                COALESCE(NULLIF(TRIM(workout_number), ''), '-1') AS workout_number,
                COALESCE("date", '1900-01-01')                  AS "date",
                COALESCE(NULLIF(TRIM(set_number), ''), '0')     AS set_number,
                COALESCE(
                    NULLIF(
                        regexp_replace(
                            upper(substr(TRIM(exercise), 1, 1)) || lower(substr(TRIM(exercise), 2)),
                            '\s+', ' ', 'g'
                        ),
                        ''
                    ),
                    'N/A'
                )                                               AS exercise,
                COALESCE(NULLIF(TRIM(reps), ''), '0')           AS reps,
                "load"                                          AS load,
                COALESCE(NULLIF(TRIM(resistance_type), ''), 'N/A') AS resistance_type,
                COALESCE(NULLIF(TRIM(set_type), ''), 'N/A')        AS set_type,
                COALESCE(NULLIF(TRIM("comments"), ''), 'N/A')      AS comments,
                COALESCE(NULLIF(TRIM(workout_type), ''), 'N/A')    AS workout_type
            FROM staging_layer.workouts
        )
        INSERT INTO cleansing_layer.cl_workouts (
            id,
            workout_number,
            "date",
            set_number,
            exercise,
            reps,
            "load",
            resistance_type,
            set_type,
            "comments",
            workout_type,
            created_at
        )
        SELECT
            nextval('cleansing_layer.workout_id_seq'),
            c.workout_number,
            c."date",
            c.set_number,
            c.exercise,
            c.reps,
            c.load,
            c.resistance_type,
            c.set_type,
            c.comments,
            c.workout_type,
            now()
        FROM cleansed c
        WHERE NOT EXISTS (
            SELECT 1
            FROM cleansing_layer.cl_workouts tgt
            WHERE tgt.workout_number = c.workout_number
              AND tgt."date" = c."date"
              AND tgt.set_number = c.set_number
              AND tgt.exercise = c.exercise
        );

        GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        RAISE NOTICE '% rows were inserted into cleansing_layer.cl_workouts', v_rows_affected;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_message_text = MESSAGE_TEXT,
                v_detail_text  = PG_EXCEPTION_DETAIL,
                v_hint_text    = PG_EXCEPTION_HINT;
            v_status_code := 2;
            RAISE NOTICE 'Error occured: %, Details: %, Hint: %', v_message_text, v_detail_text, v_hint_text;
    END;
END;
$$;

CALL load_workouts_to_cl();
