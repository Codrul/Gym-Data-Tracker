CREATE OR REPLACE PROCEDURE clean_resistance_types()
LANGUAGE plpgsql
AS $$
DECLARE
  rows_deleted INT := 0;
  rows_updated_special_chars INT := 0;
  rows_updated_capitalize INT := 0;
  total_changed INT := 0;
BEGIN 
  -- deduplicate rows
  WITH duplicates AS (
    SELECT resistance_src_id, min(resistance_src_id) AS min_id 
    FROM cleansing_layer.cl_resistance_types
    GROUP BY resistance_src_id 
    HAVING COUNT(*) > 1
  )
  DELETE FROM cleansing_layer.cl_resistance_types rt
  USING duplicates d 
  WHERE rt.resistance_src_id = d.resistance_src_id
    AND rt.resistance_src_id <> d.min_id;

  GET DIAGNOSTICS rows_deleted = ROW_COUNT;


  -- remove special chars in multiple columns
  UPDATE cleansing_layer.cl_resistance_types
  SET 
    resistance_type = regexp_replace(resistance_type, '[^0-9A-Za-z\s\-\.,]', '', 'g'),
    resistance_category = regexp_replace(resistance_category, '[^0-9A-Za-z\s\-\.,]', '', 'g')
  WHERE
    resistance_type ~ '[^0-9A-Za-z\s\-\.,]' OR
    resistance_category ~ '[^0-9A-Za-z\s\-\.,]';

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


   total_changed := rows_deleted + rows_updated_special_chars + rows_updated_capitalize;

    RAISE NOTICE '[cl_resistance_types] Rows deleted: %, (special_chars): %, (capitalize): %, Total changed: %',
                 rows_deleted, rows_updated_special_chars, rows_updated_capitalize, total_changed;

END;
$$;

CALL clean_resistance_types();
