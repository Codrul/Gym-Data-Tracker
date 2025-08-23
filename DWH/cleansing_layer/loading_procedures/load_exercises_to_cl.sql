CREATE OR REPLACE PROCEDURE load_exercises_to_cl()
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
	UPDATE cleansing_layer.cl_exercises tgt
    SET
        exercise_name = em.exercise_name,
        exercise_movement_type = em.exercise_movement_type,
		exercise_bodysplit = em.exercise_bodysplit,
        updated_at    = now()
    FROM staging_layer.exercises em
    WHERE tgt.exercise_src_id = em.exercise_id
      AND (
            tgt.exercise_name IS DISTINCT FROM em.exercise_name OR
        tgt.exercise_movement_type IS DISTINCT FROM em.exercise_movement_type OR 
		tgt.exercise_bodysplit IS DISTINCT FROM em.exercise_bodysplit 
      );
	GET DIAGNOSTICS v_rows_updated = ROW_COUNT;

    INSERT INTO cleansing_layer.cl_exercises(
      exercise_id,
	  exercise_src_id,
      exercise_name,
      exercise_movement_type,
      exercise_bodysplit,
      created_at
    )
    SELECT 
        nextval('cleansing_layer.exercise_id_seq') as exercise_id,
        COALESCE(exercise_id, '-1') as exercise_src_id,
        COALESCE(exercise_name, 'N/A'),
        COALESCE(exercise_movement_type, 'N/A'),
        COALESCE(exercise_bodysplit, 'N/A'),
        now() as created_at
    FROM (
      SELECT DISTINCT 
          e.exercise_id as exercise_id,
          e.exercise_name as exercise_name,
          e.exercise_movement_type as exercise_movement_type,
          e.exercise_bodysplit as exercise_bodysplit
      FROM staging_layer.exercises e
    ) src
    WHERE NOT EXISTS(
      SELECT 1 
      FROM cleansing_layer.cl_exercises tgt
      WHERE tgt.exercise_src_id = src.exercise_id
    );
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RAISE NOTICE '% rows were inserted and % updated into cleansing_layer.cl_exercises', v_rows_affected, v_rows_updated;

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

CALL load_exercises_to_cl();

