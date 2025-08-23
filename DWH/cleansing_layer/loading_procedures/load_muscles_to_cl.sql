CREATE OR REPLACE PROCEDURE load_muscles_to_cl()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_inserted INT := 0;
    v_rows_updated INT := 0;
    v_message_text TEXT;
    v_detail_text TEXT;
    v_hint_text TEXT;
    v_status_code INT;
BEGIN
  BEGIN

    UPDATE cleansing_layer.cl_muscles tgt
    SET
        muscle_name  = COALESCE(src.muscle_name, tgt.muscle_name),
        muscle_group = COALESCE(src.muscle_group, tgt.muscle_group),
        updated_at   = now()
    FROM staging_layer.muscles src
    WHERE tgt.muscle_src_id = src.muscle_id
      AND (
            tgt.muscle_name IS DISTINCT FROM src.muscle_name
         OR tgt.muscle_group IS DISTINCT FROM src.muscle_group
      );
    GET DIAGNOSTICS v_rows_updated = ROW_COUNT;

    INSERT INTO cleansing_layer.cl_muscles(
        muscle_id,
        muscle_src_id,
        muscle_name,
        muscle_group,
        created_at
    )
    SELECT 
        nextval('cleansing_layer.muscle_id_seq') as muscle_id,
        COALESCE(muscle_id,'-1') as muscle_src_id,
        COALESCE(muscle_name,'N/A'),
        COALESCE(muscle_group,'N/A'),
        now() as created_at
    FROM (
        SELECT DISTINCT 
            m.muscle_id,
            m.muscle_name,
            m.muscle_group
        FROM staging_layer.muscles m
    ) src
    WHERE NOT EXISTS (
        SELECT 1
        FROM cleansing_layer.cl_muscles tgt
        WHERE tgt.muscle_src_id = src.muscle_id
    );
    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    RAISE NOTICE '% rows inserted and % rows updated into cleansing_layer.cl_muscles', v_rows_inserted, v_rows_updated;

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


CALL load_muscles_to_cl();
