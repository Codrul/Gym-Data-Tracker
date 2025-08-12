CREATE OR REPLACE PROCEDURE load_resistances_to_cl()
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
    INSERT INTO cleansing_layer.cl_resistance_types(
      resistance_id,
      resistance_src_id,
      resistance_type,
      resistance_category,
      created_at
    )
    SELECT  
        nextval('cleansing_layer.resistance_id_seq') as resistance_id,
        resistance_id as resistance_src_id,
        resistance_type,
        resistance_category,
        now() as created_at
    FROM (
      SELECT DISTINCT 
        resistance_id,
        resistance_type,
        resistance_category
      FROM staging_layer.resistance_types r 
    ) src
    WHERE NOT EXISTS(
      SELECT 1 
      FROM cleansing_layer.cl_resistance_types tgt
      WHERE tgt.resistance_src_id = src.resistance_id
    );
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RAISE NOTICE '% rows were inserted or updated into cleansing_layer.cl_exercises', v_rows_affected;

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

CALL load_resistances_to_cl();

