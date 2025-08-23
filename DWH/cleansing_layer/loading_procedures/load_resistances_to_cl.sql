CREATE OR REPLACE PROCEDURE load_resistances_to_cl()
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

    UPDATE cleansing_layer.cl_resistance_types tgt
    SET
        resistance_type     = COALESCE(src.resistance_type, tgt.resistance_type),
        resistance_category = COALESCE(src.resistance_category, tgt.resistance_category),
        updated_at          = now()
    FROM staging_layer.resistance_types src
    WHERE tgt.resistance_src_id = src.resistance_id
      AND (
            tgt.resistance_type IS DISTINCT FROM src.resistance_type
         OR tgt.resistance_category IS DISTINCT FROM src.resistance_category
      );
    GET DIAGNOSTICS v_rows_updated = ROW_COUNT;


    INSERT INTO cleansing_layer.cl_resistance_types(
        resistance_id,
        resistance_src_id,
        resistance_type,
        resistance_category,
        created_at
    )
    SELECT 
        nextval('cleansing_layer.resistance_id_seq') as resistance_id,
        COALESCE(resistance_id,'-1') as resistance_src_id,
        COALESCE(resistance_type,'N/A'),
        COALESCE(resistance_category,'N/A'),
        now() as created_at
    FROM (
        SELECT DISTINCT 
            resistance_id,
            resistance_type,
            resistance_category
        FROM staging_layer.resistance_types
    ) src
    WHERE NOT EXISTS (
        SELECT 1
        FROM cleansing_layer.cl_resistance_types tgt
        WHERE tgt.resistance_src_id = src.resistance_id
    );
    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    RAISE NOTICE '% rows inserted and % rows updated into cleansing_layer.cl_resistance_types', v_rows_inserted, v_rows_updated;

  EXCEPTION
    WHEN OTHERS THEN 
      GET STACKED DIAGNOSTICS
        v_message_text = MESSAGE_TEXT,
        v_detail_text = PG_EXCEPTION_DETAIL,
        v_hint_text = PG_EXCEPTION_HINT;
      v_status_code := 2;
      RAISE EXCEPTION 'Error occurred: %, Details: %, Hint: %', v_message_text, v_detail_text, v_hint_text;
  END;
END;
$$;


CALL load_resistances_to_cl();
