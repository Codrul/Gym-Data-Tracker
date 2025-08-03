CREATE OR REPLACE PROCEDURE load_workouts_to_cl()
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
    INSERT INTO cleansing_layer.cl_workouts(
      workout_id,
      workout_src_id,
      date,
      set_number,
      exercise,
      reps,
      load,
      resistance_type,
      set_type,
      comments,
      workout_type,
      created_at
    )
    SELECT 
        nextval('cleansing_layer.workout_id_seq') as workout_id,
        workout_id as workout_src_id,
        "date",
        set_number,
        exercise,
        reps,
        load,
        resistance_type,
        set_type,
        comments,
        workout_type,
        now() as created_at
    FROM (
      SELECT DISTINCT 
        w.workout_id,
        w."date",
        w.set_number,
        w.exercise,
        w.reps,
        w.load,
        w.resistance_type,
        w.set_type,
        w.comments,
        w.workout_type
      FROM staging_layer.workouts w 
    ) src
    WHERE NOT EXISTS(
      SELECT 1 
      FROM cleansing_layer.cl_workouts tgt
      WHERE tgt.workout_src_id = src.workout_id
    );
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RAISE NOTICE '% rows were inserted or updated into cleansing_layer.cl_workouts', v_rows_affected;

  EXCEPTION
    WHEN OTHERS THEN 
      GET STACKED DIAGNOSTICS
        v_message_text = MESSAGE_TEXT,
        v_detail_text = PG_EXCEPTION_DETAIL,
        v_hint_text = PG_EXCEPTION_HINT;
      v_status_code := 2; -- error status code ofc 
      RAISE NOTICE 'Error occured: %, Details: %, Hint: %', v_message_text, v_detail_text, v_hint_text;
	END;
	END;
$$;

CALL load_workouts_to_cl();

