CREATE OR REPLACE PROCEDURE load_exercises_to_3nf()
LANGUAGE plpgsql
AS $$
DECLARE
	v_rows_affected INT := 0;
	v_message_text TEXT;
	v_detail_text TEXT;
    v_hint_text TEXT;
	v_status_code INT;
BEGIN
    INSERT INTO bl_3nf.CE_exercises
    (exercise_id, exercise_src_id, exercise_name, exercise_movement_type, exercise_bodysplit, TA_created_at, TA_updated_at)
    VALUES 
    (-1, '-1', 'N/A', 'N/A', 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    ON CONFLICT (exercise_id) DO NOTHING;
    

  BEGIN
    MERGE INTO bl_3nf.CE_exercises as tgt
    USING cleansing_layer.cl_exercises as src 
    ON tgt.exercise_src_id = src.exercise_id    
    WHEN MATCHED AND (
    tgt.exercise_name <> src.exercise_name OR 
    tgt.exercise_movement_type <> src.exercise_movement_type OR 
    tgt.exercise_bodysplit <> src.exercise_bodysplit
    )
        THEN UPDATE SET 
            exercise_name = src.exercise_name,
            exercise_movement_type = src.exercise_movement_type,
            exercise_bodysplit = src.exercise_bodysplit,
            TA_updated_at = NOW()
    WHEN NOT MATCHED 
        THEN INSERT (
            exercise_id, 
            exercise_src_id,
            exercise_name, 
            exercise_movement_type,
            exercise_bodysplit,
            TA_created_at, 
            TA_updated_at
        )
        VALUES (
            nextval('bl_3nf.exercise_id_seq'),
            COALESCE(src.exercise_id, '-1'),
            COALESCE(src.exercise_name, 'N/A'),
            COALESCE(src.exercise_movement_type, 'N/A'),
            COALESCE(src.exercise_bodysplit, 'N/A'),
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        );

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RAISE NOTICE '% rows were inserted or updated into bl_3nf.CE_exercises', v_rows_affected;

  EXCEPTION
    WHEN OTHERS THEN 
      GET STACKED DIAGNOSTICS
        v_message_text = MESSAGE_TEXT,
        v_detail_text = PG_EXCEPTION_DETAIL,
        v_hint_text = PG_EXCEPTION_HINT;
      v_status_code := 2; -- error status code ofc 
      RAISE EXCEPTION 'Error occured: %, Details: %, Hint: %', v_message_text, v_detail_text, v_hint_text;
	END;
	END;
$$;




