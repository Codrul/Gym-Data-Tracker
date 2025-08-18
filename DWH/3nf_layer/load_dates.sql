CREATE OR REPLACE PROCEDURE load_dates()
LANGUAGE plpgsql
AS $$
DECLARE
    v_current_date DATE;
    v_rows_affected INT := 0;
    v_message_text TEXT;
    v_detail_text TEXT;
    v_hint_text TEXT;
    v_status_code INT;
BEGIN
  BEGIN
    -- Insert default row
    INSERT INTO bl_3nf.ce_dates (
        "DATE",
        day_name_in_week,
        day_number_in_month,
        calendar_month_number,
        calendar_month_desc,
        calendar_year,
        is_weekend
    )
    VALUES (
        DATE '1900-01-01',
        'N/A',
        -1,
        -1,
        'N/A',
        -1,
        'N'
    )
    ON CONFLICT ("DATE") DO NOTHING;

    -- Loop over all dates from May 1, 2025 to December 31, 2030
    v_current_date := DATE '2025-05-01';
    WHILE v_current_date <= DATE '2030-12-31' LOOP
        INSERT INTO bl_3nf.ce_dates (
            "DATE",
            day_name_in_week,
            day_number_in_month,
            calendar_month_number,
            calendar_month_desc,
            calendar_year,
            is_weekend
        )
        VALUES (
            COALESCE(v_current_date, DATE '1900-01-01'),
            COALESCE(TRIM(TO_CHAR(v_current_date, 'Day')), 'N/A'),
            COALESCE(EXTRACT(DAY FROM v_current_date)::INT, -1),
            COALESCE(EXTRACT(MONTH FROM v_current_date)::INT, -1),
            COALESCE(TRIM(TO_CHAR(v_current_date, 'Month')), 'N/A'),
            COALESCE(EXTRACT(YEAR FROM v_current_date)::INT, -1),
            CASE
                WHEN EXTRACT(DOW FROM v_current_date) IN (0,6) THEN 'Y'
                ELSE 'N'
            END
        )
        ON CONFLICT ("DATE") DO NOTHING;

        v_current_date := v_current_date + INTERVAL '1 day';
    END LOOP;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RAISE NOTICE '% rows were inserted into bl_3nf.ce_dates', v_rows_affected;

  EXCEPTION
    WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_message_text = MESSAGE_TEXT,
        v_detail_text = PG_EXCEPTION_DETAIL,
        v_hint_text = PG_EXCEPTION_HINT;
      v_status_code := 2; -- error status code
      RAISE NOTICE 'Error occurred: %, Details: %, Hint: %', v_message_text, v_detail_text, v_hint_text;
  END;
END;
$$;

-- Call the procedure
CALL load_dates();
