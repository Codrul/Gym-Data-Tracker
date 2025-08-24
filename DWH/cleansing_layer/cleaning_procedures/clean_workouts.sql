CREATE OR REPLACE PROCEDURE clean_workouts()
LANGUAGE plpgsql
AS $$
DECLARE
    rows_deleted            	INT := 0;
    rows_updated_date        	INT := 0;
    rows_updated_load_dot    	INT := 0;
    rows_updated_special_chars	INT := 0;
    rows_updated_capitalize  	INT := 0;
    rows_updated_load_unit   	INT := 0;
	rows_updated_id				INT :=0;
    total_changed           	INT := 0;
BEGIN
    
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
	SET "load" = regexp_replace("load", ',', '.', 'g')
	WHERE "load" ~ '[0-9]' AND "load" LIKE '%,%';


    GET DIAGNOSTICS rows_updated_load_dot = ROW_COUNT;

    -- remove special chars in multiple columns
    UPDATE cleansing_layer.cl_workouts
    SET exercise = regexp_replace(exercise, '[^0-9A-Za-z\s\-\.,]', '', 'g'),
        workout_type = regexp_replace(workout_type, '[^0-9A-Za-z\s\-\.,]', '', 'g')
    WHERE (exercise ~ '[^0-9A-Za-z\s\-\.,]' AND exercise != 'N/A')
       OR (workout_type ~ '[^0-9A-Za-z\s\-\.,]' AND workout_type != 'N/A');

    GET DIAGNOSTICS rows_updated_special_chars = ROW_COUNT;

    -- trim spaces and capitalize first letter
  	UPDATE cleansing_layer.cl_workouts cw
	SET exercise = regexp_replace(
	                   upper(substr(cw.exercise, 1, 1)) || lower(substr(cw.exercise, 2)),
	                   '\s+', ' ', 'g'
	               ),
	    resistance_type = regexp_replace(
	                   upper(substr(cw.resistance_type, 1, 1)) || lower(substr(cw.resistance_type, 2)),
	                   '\s+', ' ', 'g'
	               )
	WHERE (exercise != 'N/A'
	       AND exercise <> regexp_replace(
	                           upper(substr(cw.exercise, 1, 1)) || lower(substr(cw.exercise, 2)),
	                           '\s+', ' ', 'g'
	                       ))
	   OR (resistance_type != 'N/A'
	       AND resistance_type <> regexp_replace(
	                           upper(substr(cw.resistance_type, 1, 1)) || lower(substr(cw.resistance_type, 2)),
	                           '\s+', ' ', 'g'
	                       ));


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

	UPDATE cleansing_layer.cl_workouts
	SET "load" = COALESCE(NULLIF(TRIM("load"), ''), '0')
	WHERE "load" = '';


	UPDATE cleansing_layer.cl_workouts w
	SET exercise_id = e.exercise_id
	FROM cleansing_layer.cl_exercises e
	WHERE w.exercise = e.exercise_name
	  AND (w.exercise_id IS NULL OR w.exercise_id = '-1');
	
	UPDATE cleansing_layer.cl_workouts w
	SET resistance_id = r.resistance_id
	FROM cleansing_layer.cl_resistance_types r
	WHERE w.resistance_type = r.resistance_type
	  AND (w.resistance_id IS NULL OR w.resistance_id = '-1');

	-- in case there are left non-coalesce values
	UPDATE cleansing_layer.cl_workouts
	SET 
	    reps = '0'
	WHERE reps IS NULL OR TRIM(reps) = '';

	UPDATE cleansing_layer.cl_workouts
	SET 
	    "load" = '0'
	WHERE "load" IS NULL OR TRIM("load") = '';

	UPDATE cleansing_layer.cl_workouts
	SET 
		set_number = '0'
	WHERE set_number IS NULL OR trim(set_number) = '';

	UPDATE cleansing_layer.cl_workouts
	SET
		exercise = 'N/A'
	WHERE exercise IS NULL OR trim(exercise) = '';

	UPDATE cleansing_layer.cl_workouts
	SET 
		resistance_type = 'N/A'
	WHERE resistance_type IS NULL or TRIM(resistance_type) = '';

	UPDATE cleansing_layer.cl_workouts
	SET 
		set_type = 'N/A'
	WHERE set_type IS NULL or TRIM(set_type) = '';
	
	UPDATE cleansing_layer.cl_workouts
	SET 
		"comments" = 'N/A'
	WHERE "comments" IS NULL or TRIM("comments") = '';

	UPDATE cleansing_layer.cl_workouts
	SET 
		workout_type = 'N/A'
	WHERE workout_type IS NULL or TRIM(workout_type) = '';

	UPDATE cleansing_layer.cl_workouts
	SET 
		"date" = '1900-01-01'
	WHERE "date" IS NULL or TRIM("date") = '';


	
	GET DIAGNOSTICS rows_updated_id = ROW_COUNT;

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

    total_changed := rows_deleted + rows_updated_date + rows_updated_load_dot
                   + rows_updated_special_chars + rows_updated_capitalize + rows_updated_load_unit + rows_updated_id;

    RAISE NOTICE '[cl_workouts] Rows deleted: %, Rows updated (date): %, (load_dot): %, (special_chars): %, (capitalize): %, (load_unit): %, (ID updates): %, Total changed: %',
                 rows_deleted, rows_updated_date, rows_updated_load_dot, rows_updated_special_chars,
                 rows_updated_capitalize, rows_updated_load_unit, rows_updated_id, total_changed;

END;
$$;

CALL clean_workouts();
