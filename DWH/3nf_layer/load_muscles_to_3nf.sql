CREATE OR REPLACE PROCEDURE load_muscles_to_3nf()
LANGUAGE plpgsql
AS $$
DECLARE 
    v_rows_affected INT := 0;
    v_message_text TEXT;
    v_detail_text TEXT;
    v_hint_text TEXT;
    v_status_code INT;
BEGIN
    INSERT INTO bl_3nf.CE_muscles
    (muscle_id, muscle_src_id, muscle_name, muscle_group, ta_created_at, ta_updated_at)
    VALUES (
        -1, '-1', 'N/A', 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    ON CONFLICT (muscle_id) DO NOTHING;


    BEGIN
        MERGE INTO bl_3nf.CE_muscles as tgt
        USING cleansing_layer.cl_muscles as src
        ON tgt.muscle_src_id = src.muscle_id
        WHEN MATCHED AND (
        tgt.muscle_name <> src.muscle_name OR
        tgt.muscle_group <> src.muscle_group 
        )
            THEN UPDATE SET 
                muscle_name = src.muscle_name,
                muscle_group = src.muscle_group,
                TA_updated_at = NOW()
        WHEN NOT MATCHED 
            THEN INSERT (
                muscle_id,
                muscle_src_id,
                muscle_name,
                muscle_group,
                TA_created_at,
                TA_updated_at
            )
            VALUES (
                nextval('bl_3nf.muscle_id_seq'),
                COALESCE(src.muscle_id, '-1'),
                COALESCE(src.muscle_name, 'N/A'),
                COALESCE(src.muscle_group, 'N/A'),
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            );

        GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        RAISE NOTICE '% rows were inserted or updated into bl_3nf.CE_muscles', v_rows_affected;

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
