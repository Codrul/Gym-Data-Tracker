CREATE OR REPLACE PROCEDURE load_muscles_to_cl()
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
    INSERT INTO cleansing_layer.cl_muscles(
      muscle_id,
	    muscle_src_id,
      muscle_name,
      muscle_group,
      created_at
    )
    SELECT 
        nextval('cleansing_layer.muscle_id_seq') as muscle_id,
        muscle_id as muscle_src_id,
        muscle_name,
        muscle_group,
        now() as created_at
    FROM (
      SELECT DISTINCT 
          m.muscle_id as muscle_id,
          m.muscle_name as muscle_name,
          m.muscle_group as muscle_group
      FROM staging_layer.muscles m
    ) src
    WHERE NOT EXISTS(
      SELECT 1 
      FROM cleansing_layer.cl_muscles tgt
      WHERE tgt.muscle_src_id = src.muscle_id
    );
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RAISE NOTICE '% rows were inserted or updated into cleansing_layer.cl_muscles', v_rows_affected;

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

CALL load_muscles_to_cl();

