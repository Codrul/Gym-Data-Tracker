CREATE OR REPLACE PROCEDURE clean_exercise_muscle()
LANGUAGE plpgsql
AS $$
DECLARE
  	rows_deleted             INT := 0;
    rows_updated_special_chars INT := 0;
    rows_updated_capitalize  INT := 0;
    total_changed           INT := 0;
BEGIN 
  -- deduplicate rows
  WITH duplicates AS (
    SELECT exercise_src_id, exercise_name, muscle_src_id, muscle_name
    FROM cleansing_layer.cl_exercise_muscle
    GROUP BY exercise_src_id, exercise_name, muscle_src_id, muscle_name
    HAVING COUNT(*) > 1
  )
  DELETE FROM cleansing_layer.cl_exercise_muscle em
  USING duplicates d 
  WHERE em.exercise_src_id = d.exercise_src_id
	AND em.exercise_name = d.exercise_name
	AND em.muscle_src_id = d.muscle_src_id
	AND em.muscle_name = d.muscle_name;

  GET DIAGNOSTICS rows_deleted = ROW_COUNT;


  -- remove special chars in multiple columns
  UPDATE cleansing_layer.cl_exercise_muscle
  SET 
    exercise_name = regexp_replace(exercise_name, '[^0-9A-Za-z\s\-\.,]', '', 'g'),
    muscle_name = regexp_replace(muscle_name, '[^0-9A-Za-z\s\-\.,]', '', 'g'),
	muscle_role = regexp_replace(muscle_role, '[^0-9A-Za-z\s\-\.,]', '', 'g')
  WHERE
   	exercise_name <> regexp_replace(exercise_name, '[^0-9A-Za-z\s\-\.,]', '', 'g')
	AND muscle_name <> regexp_replace(muscle_name, '[^0-9A-Za-z\s\-\.,]', '', 'g')
	AND muscle_role <> regexp_replace(muscle_role, '[^0-9A-Za-z\s\-\.,]', '', 'g');

  GET DIAGNOSTICS rows_updated_special_chars = ROW_COUNT;

  -- trim spaces and capitalize first letter
  UPDATE cleansing_layer.cl_exercise_muscle
  SET exercise_name = regexp_replace(
                   upper(substr(exercise_name, 1, 1)) || lower(substr(exercise_name, 2)), 
                   '\s+', ' ', 'g'
                ),
		muscle_name = regexp_replace(
                   upper(substr(muscle_name, 1, 1)) || lower(substr(muscle_name, 2)), 
                   '\s+', ' ', 'g'
                ),
		muscle_role = regexp_replace(
                   upper(substr(muscle_role, 1, 1)) || lower(substr(muscle_role, 2)), 
                   '\s+', ' ', 'g'
                )
  WHERE exercise_name IS NOT NULL
    AND exercise_name <> regexp_replace(
                   upper(substr(exercise_name, 1, 1)) || lower(substr(exercise_name, 2)), 
                   '\s+', ' ', 'g'
                )
	OR muscle_name IS NOT NULL 
	AND muscle_name <> regexp_replace(
                   upper(substr(muscle_name, 1, 1)) || lower(substr(muscle_name, 2)), 
                   '\s+', ' ', 'g'
                )
	OR muscle_role IS NOT NULL
	AND muscle_role <> regexp_replace(
                   upper(substr(muscle_role, 1, 1)) || lower(substr(muscle_role, 2)), 
                   '\s+', ' ', 'g'
                );

  GET DIAGNOSTICS rows_updated_capitalize = ROW_COUNT;



  
    total_changed := rows_deleted + rows_updated_special_chars + rows_updated_capitalize;

    RAISE NOTICE '[cl_exercise_muscle] Rows deleted: %, (special_chars): %, (capitalize): %, Total changed: %',
                 rows_deleted, rows_updated_special_chars, rows_updated_capitalize, total_changed;

END;
$$;

CALL clean_exercise_muscle();
