CREATE OR REPLACE PROCEDURE load_resistance_types_to_3nf()
LANGUAGE plpgsql
AS $$
DECLARE
	v_rows_affected INT := 0;
	v_message_text TEXT;
	v_detail_text TEXT;
    v_hint_text TEXT;
	v_status_code INT;
BEGIN
    INSERT INTO bl_3nf.CE_resistance_types
    (resistance_id, resistance_src_id, resistance_type, resistance_category, TA_created_at, TA_updated_at)
    VALUES 
    (-1, '-1', 'N/A', 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    ON CONFLICT (resistance_id) DO NOTHING;
    

  BEGIN
    MERGE INTO bl_3nf.CE_resistance_types as tgt
    USING cleansing_layer.cl_resistance_types as src 
    ON tgt.resistance_src_id = src.resistance_id    
    WHEN MATCHED AND (
    tgt.resistance_type <> src.resistance_type OR 
    tgt.resistance_category <> src.resistance_category
    )
        THEN UPDATE SET 
            resistance_type = src.resistance_type, 
            resistance_category = src.resistance_category,
            TA_updated_at = NOW()
    WHEN NOT MATCHED 
        THEN INSERT (
            resistance_id, 
            resistance_src_id,
            resistance_type, 
            resistance_category,
            TA_created_at, 
            TA_updated_at
        )
        VALUES (
            nextval('bl_3nf.resistance_id_seq'),
            COALESCE(src.resistance_id, '-1'),
            COALESCE(src.resistance_type, 'N/A'),
            COALESCE(src.resistance_category, 'N/A'),
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        );

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RAISE NOTICE '% rows were inserted or updated into bl_3nf.CE_resistance_types', v_rows_affected;

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




