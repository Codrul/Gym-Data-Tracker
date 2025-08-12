CREATE OR REPLACE PROCEDURE clean_workouts()
LANGUAGE plpgsql
AS $$
DECLARE
    rows_deleted             INT := 0;
    rows_updated_date        INT := 0;
    rows_updated_load_dot    INT := 0;
    rows_updated_special_chars INT := 0;
    rows_updated_capitalize  INT := 0;
    rows_updated_load_unit   INT := 0;
    total_changed           INT := 0;
BEGIN
    -- deduplicate rows
    WITH duplicates AS (
        SELECT workout_number, "date", set_number, exercise, min(id) AS min_id
        FROM cleansing_layer.cl_workouts
        GROUP BY workout_number, "date", set_number, exercise
        HAVING COUNT(*) > 1
    )
    DELETE FROM cleansing_layer.cl_workouts cw
    USING duplicates d
    WHERE cw.workout_number = d.workout_number
      AND cw."date" = d."date"
      AND cw.set_number = d.set_number
      AND cw.exercise = d.exercise
      AND cw.id <> d.min_id;
    GET DIAGNOSTICS rows_deleted = ROW_COUNT;

    -- standardize date formats
    UPDATE cleansing_layer.cl_workouts
    SET "date" = CASE
                    WHEN "date" ~ '^\d{2}/\d{2}/\d{4}$' THEN to_char(to_date("date", 'MM/DD/YYYY'), 'YYYY-MM-DD')
                    WHEN "date" ~ '^\d{4}.\d{2}.\d{2}$' THEN to_char(to_date("date", 'YYYY.MM.DD'), 'YYYY-MM-DD')
                    ELSE "date"
                 END
    WHERE "date" !~ '^\d{4}-\d{2}-\d{2}$';
    GET DIAGNOSTICS rows_updated_date = ROW_COUNT;

    -- turn commas into dots for numeric values
    UPDATE cleansing_layer.cl_workouts
    SET "load" = new_value
    FROM (
        SELECT id,
               CASE
                   WHEN regexp_replace(regexp_replace("load", '[ ]', '', 'g'), ',', '.', 'g') ~ '^[0-9]+(\.[0-9]+)?$'
                   THEN regexp_replace(regexp_replace("load", '[ ]', '', 'g'), ',', '.', 'g')::numeric::text
                   ELSE '0'
               END AS new_value
        FROM cleansing_layer.cl_workouts
        WHERE "load" IS NOT NULL
    ) sub
    WHERE cleansing_layer.cl_workouts.id = sub.id
      AND (cleansing_layer.cl_workouts."load" IS DISTINCT FROM sub.new_value);
    GET DIAGNOSTICS rows_updated_load_dot = ROW_COUNT;

    -- remove special chars in multiple columns
    UPDATE cleansing_layer.cl_workouts
    SET exercise = regexp_replace(exercise, '[^0-9A-Za-z\s\-\.,]', '', 'g'),
        "comments" = regexp_replace("comments", '[^0-9A-Za-z\s\-\.,]', '', 'g'),
        workout_type = regexp_replace(workout_type, '[^0-9A-Za-z\s\-\.,]', '', 'g')
    WHERE exercise ~ '[^0-9A-Za-z\s\-\.,]'
       OR "comments" ~ '[^0-9A-Za-z\s\-\.,]'
       OR workout_type ~ '[^0-9A-Za-z\s\-\.,]';
    GET DIAGNOSTICS rows_updated_special_chars = ROW_COUNT;

    -- trim spaces and capitalize first letter
    UPDATE cleansing_layer.cl_workouts cw
    SET exercise = regexp_replace(
                       upper(substr(cw.exercise, 1, 1)) || lower(substr(cw.exercise, 2)),
                       '\s+', ' ', 'g'
                   )
    WHERE exercise IS NOT NULL
      AND exercise <> regexp_replace(
                          upper(substr(cw.exercise, 1, 1)) || lower(substr(cw.exercise, 2)),
                          '\s+', ' ', 'g'
                      );
    GET DIAGNOSTICS rows_updated_capitalize = ROW_COUNT;

    -- separate load and unit
    UPDATE cleansing_layer.cl_workouts
    SET unit = CASE
                   WHEN "load" ~* 'kg' THEN 'KG'
                   WHEN "load" ~* 'lbs' THEN 'LBS'
                   ELSE 'N/A'
               END,
        "load" = regexp_replace("load", '[^0-9.]', '', 'g')
    WHERE "load" ~* '(kg|lbs)';
    GET DIAGNOSTICS rows_updated_load_unit = ROW_COUNT;

    total_changed := rows_deleted + rows_updated_date + rows_updated_load_dot
                   + rows_updated_special_chars + rows_updated_capitalize + rows_updated_load_unit;

    RAISE NOTICE '[cl_workouts] Rows deleted: %, Rows updated (date): %, (load_dot): %, (special_chars): %, (capitalize): %, (load_unit): %, Total changed: %',
                 rows_deleted, rows_updated_date, rows_updated_load_dot, rows_updated_special_chars,
                 rows_updated_capitalize, rows_updated_load_unit, total_changed;

END;
$$;

CALL clean_workouts();
