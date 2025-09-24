CREATE OR REPLACE PROCEDURE load_workouts_to_3nf()
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
    
    INSERT INTO bl_3nf.fact_workouts(
        id,
        src_id,
        workout_number,
        date_id,
        set_number,
        exercise_id,
        reps,
        "load",
        "unit",
        resistance_id,
        set_type,
        "comments",
        workout_type,
        ta_created_at
    )
    SELECT 
        nextval('bl_3nf.workout_id_seq') AS id,
		COALESCE(w.id, 'N/A') AS src_id,
		COALESCE(w.workout_number::INT, -1) AS workout_number,
		COALESCE(w."date"::DATE, DATE '1900-01-01') AS date_id,
		COALESCE(w.set_number::INT, -1) AS set_number,
		COALESCE(e.exercise_id::BIGINT, -1) AS exercise_id,
		COALESCE(w.reps::NUMERIC, -1) AS reps,
		COALESCE(w."load"::NUMERIC, -1) AS "load",
		COALESCE(w."unit", 'N/A') AS "unit",
		COALESCE(r.resistance_id::BIGINT, -1) AS resistance_id,
		COALESCE(w.set_type, 'N/A') AS set_type,
		COALESCE(w."comments", 'N/A') AS "comments",
		COALESCE(w.workout_type, 'N/A') AS workout_type,
        CURRENT_TIMESTAMP AS ta_created_at
    FROM cleansing_layer.cl_workouts w 
    LEFT JOIN bl_3nf.ce_exercises e 
           ON e.exercise_src_id = w.exercise_id
    LEFT JOIN bl_3nf.ce_resistance_types r 
           ON r.resistance_src_id = w.resistance_id
    WHERE NOT EXISTS (
        SELECT 1 
        FROM bl_3nf.fact_workouts tgt
        WHERE tgt.src_id = w.id
    );

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RAISE NOTICE '% rows were inserted into bl_3nf.FACT_workouts', v_rows_affected;

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
