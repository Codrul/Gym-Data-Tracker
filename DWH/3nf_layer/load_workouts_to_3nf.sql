-- this procedure will need to be worked on as I need a mapping table


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
        w.id as src_id,
        w.workout_number as workout_number,
        w.date_id as date_id,
        w.set_number as set_number,
        e.exercise_id as exercise_id,
        w.reps as reps,
        w."load" as "load",
        w."unit" as "unit",
        r.resistance_id as resistance_id,
        w.set_type as set_type,
        w."comments" as "comments",
        w.workout_type as workout_type,
        CURRENT_TIME as ta_created_at
    FROM cleansing_layer.cl_workouts w 
    LEFT JOIN bl_3nf.ce_exercises e ON e.exercise_src_id = w.exercise_id
    LEFT JOIN bl_3nf.ce_resistance_types r ON r.resistance_src_id = r.resistance_id
    WHERE NOT EXISTS (
        SELECT 1 FROM bl_3nf.fact_workouts tgt
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
      RAISE NOTICE 'Error occurred: %, Details: %, Hint: %', v_message_text, v_detail_text, v_hint_text;
  END;
END;
$$;
