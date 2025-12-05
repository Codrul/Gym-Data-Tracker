 CREATE OR REPLACE PROCEDURE load_exercise_muscle_to_3nf()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INT := 0;
    v_message_text TEXT;
    v_detail_text TEXT;
    v_hint_text TEXT;
    v_status_code INT;
BEGIN
  BEGIN
    MERGE INTO bl_3nf.CE_exercise_muscle AS tgt
    USING (
        SELECT DISTINCT 
            e.exercise_id,
            em.exercise_id AS exercise_src_id,
            m.muscle_id,
            em.muscle_id AS muscle_src_id,
            em.muscle_role,
            NOW() AS ta_created_at,
            NOW() AS ta_updated_at
        FROM cleansing_layer.cl_exercise_muscle em
        JOIN bl_3nf.CE_exercises e 
            ON em.exercise_id = e.exercise_src_id
        JOIN bl_3nf.CE_muscles m 
            ON em.muscle_id = m.muscle_src_id
    ) AS src
    ON tgt.exercise_src_id = src.exercise_src_id
       AND tgt.muscle_src_id = src.muscle_src_id
    WHEN MATCHED AND (
        tgt.muscle_role <> src.muscle_role
    )
        THEN UPDATE SET 
            muscle_role = src.muscle_role,
            ta_updated_at = NOW()
    WHEN NOT MATCHED
        THEN INSERT (
            exercise_id,
            exercise_src_id,
            muscle_id,
            muscle_src_id,
            muscle_role,
            ta_created_at,
            ta_updated_at
        )
        VALUES (
            src.exercise_id,
            src.exercise_src_id,
            src.muscle_id,
            src.muscle_src_id,
            COALESCE(src.muscle_role, 'N/A'),
            NOW(),
            NOW()
        );

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RAISE NOTICE '% rows were inserted or updated into bl_3nf.CE_exercise_muscle', v_rows_affected;

  EXCEPTION
    WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_message_text = MESSAGE_TEXT,
        v_detail_text = PG_EXCEPTION_DETAIL,
        v_hint_text = PG_EXCEPTION_HINT;
      v_status_code := 2;
      RAISE EXCEPTION 'Error occurred: %, Details: %, Hint: %', v_message_text, v_detail_text, v_hint_text;
  END;
END;
$$;
