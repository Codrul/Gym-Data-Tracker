CREATE OR REPLACE PROCEDURE load_exercise_muscle_to_cl()
LANGUAGE plpgsql
AS $$
DECLARE
	v_rows_affected INT := 0;
	v_rows_updated INT := 0;
	v_message_text TEXT;
	v_detail_text TEXT;
  	v_hint_text TEXT;
	v_status_code INT;
BEGIN
  BEGIN
	UPDATE cleansing_layer.cl_exercise_muscle tgt
	SET
	    exercise_name = em.exercise_name,
	    muscle_name   = em.muscle_name,
	    muscle_role   = em.muscle_role,
	    updated_at    = now()
	FROM staging_layer.exercise_muscle em
	JOIN cleansing_layer.cl_exercises e ON em.exercise_id = e.exercise_src_id
	JOIN cleansing_layer.cl_muscles m ON em.muscle_id = m.muscle_src_id
	WHERE tgt.exercise_src_id = e.exercise_src_id
	  AND tgt.muscle_src_id = m.muscle_src_id
	  AND (tgt.exercise_name IS DISTINCT FROM em.exercise_name
	       OR tgt.muscle_name IS DISTINCT FROM em.muscle_name
	       OR tgt.muscle_role IS DISTINCT FROM em.muscle_role);
	GET DIAGNOSTICS v_rows_updated = ROW_COUNT;

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
    SELECT DISTINCT
      e.exercise_id, 
      COALESCE(em.exercise_id, '-1') as exercise_src_id,
      COALESCE(em.exercise_name, 'N/A'), 
      m.muscle_id,
      COALESCE(em.muscle_id, '-1') as muscle_src_id,
      COALESCE(em.muscle_name, 'N/A'),
      COALESCE(em.muscle_role, 'N/A'),
      now() as created_at
    FROM staging_layer.exercise_muscle em
    JOIN cleansing_layer.cl_exercises e ON em.exercise_id = e.exercise_src_id
    JOIN cleansing_layer.cl_muscles m ON em.muscle_id = m.muscle_src_id
   WHERE NOT EXISTS(
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
        v_detail_text = PG_EXCEPTION_DETAIL,
        v_hint_text = PG_EXCEPTION_HINT;
      v_status_code := 2; -- error status code ofc 
      RAISE EXCEPTION'Error occured: %, Details: %, Hint: %', v_message_text, v_detail_text, v_hint_text;
	END;
	END;
$$;


