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
        COALESCE(nextval('cleansing_layer.workout_id_seq'), '-1') AS id,
        COALESCE(workout_number, '-1') AS workout_number,
        COALESCE("date", '1900-01-01') AS "date",
        COALESCE(set_number, '-1') AS set_number,
        COALESCE(exercise, 'N/A') AS exercise,
        COALESCE(reps, '0') AS reps,
        "load" AS "load",
        COALESCE(resistance_type, 'N/A') AS resistance_type,
        COALESCE(set_type, 'N/A') AS set_type,
        COALESCE("comments", 'N/A') AS "comments",
        COALESCE(workout_type, 'N/A') AS workout_type,
        now() AS created_at
    FROM (
      SELECT DISTINCT 
        w.workout_number,
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
      WHERE tgt.workout_number = src.workout_number
		AND tgt."date" = src."date"
		AND COALESCE(TRIM(LOWER(tgt.exercise)), 'N/A') = COALESCE(TRIM(LOWER(src.exercise)), 'N/A')
		AND COALESCE(TRIM(LOWER(tgt.set_number)), '0') = COALESCE(TRIM(LOWER(src.set_number)), '0')
    );

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RAISE NOTICE '% rows were inserted into cleansing_layer.cl_workouts', v_rows_affected;

  EXCEPTION
    WHEN OTHERS THEN 
      GET STACKED DIAGNOSTICS
        v_message_text = MESSAGE_TEXT,
        v_detail_text = PG_EXCEPTION_DETAIL,
        v_hint_text = PG_EXCEPTION_HINT;
      v_status_code := 2; -- error status code
      RAISE NOTICE 'Error occured: %, Details: %, Hint: %', v_message_text, v_detail_text, v_hint_text;
	END;
END;
$$;

CALL load_workouts_to_cl();
