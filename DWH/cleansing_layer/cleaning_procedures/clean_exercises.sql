CREATE OR REPLACE PROCEDURE clean_exercises()
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
        SELECT
            exercise_src_id,
            exercise_name,
            exercise_movement_type,
            exercise_bodysplit,
            min(exercise_id) AS min_id
        FROM cleansing_layer.cl_exercises
        GROUP BY
            exercise_src_id,
            exercise_name,
            exercise_movement_type,
            exercise_bodysplit
        HAVING COUNT(*) > 1
    )
    DELETE FROM cleansing_layer.cl_exercises e
    USING duplicates d
    WHERE e.exercise_src_id = d.exercise_src_id
      AND e.exercise_name = d.exercise_name
      AND e.exercise_movement_type = d.exercise_movement_type
      AND e.exercise_id <> d.min_id;

    GET DIAGNOSTICS rows_deleted = ROW_COUNT;

    -- remove special chars in multiple columns
    UPDATE cleansing_layer.cl_exercises
    SET
        exercise_name = regexp_replace(exercise_name, '[^0-9A-Za-z\s\-\.,]', '', 'g'),
        exercise_movement_type = regexp_replace(exercise_movement_type, '[^0-9A-Za-z\s\-\.,]', '', 'g'),
        exercise_bodysplit = regexp_replace(exercise_bodysplit, '[^0-9A-Za-z\s\-\.,]', '', 'g')
    WHERE
        exercise_name ~ '[^0-9A-Za-z\s\-\.,]'
        OR exercise_movement_type ~ '[^0-9A-Za-z\s\-\.,]'
        OR exercise_bodysplit ~ '[^0-9A-Za-z\s\-\.,]';

    GET DIAGNOSTICS rows_updated_special_chars = ROW_COUNT;

    -- trim spaces and capitalize first letter
    UPDATE cleansing_layer.cl_exercises
    SET
        exercise_name = regexp_replace(
                            upper(substr(exercise_name, 1, 1)) || lower(substr(exercise_name, 2)),
                            '\s+', ' ', 'g'
                        ),
        exercise_movement_type = regexp_replace(
                                    upper(substr(exercise_movement_type, 1, 1)) || lower(substr(exercise_movement_type, 2)),
                                    '\s+', ' ', 'g'
                                ),
        exercise_bodysplit = regexp_replace(
                                upper(substr(exercise_bodysplit, 1, 1)) || lower(substr(exercise_bodysplit, 2)),
                                '\s+', ' ', 'g'
                            )
    WHERE
        (exercise_name IS NOT NULL AND exercise_name <> regexp_replace(
            upper(substr(exercise_name, 1, 1)) || lower(substr(exercise_name, 2)),
            '\s+', ' ', 'g'
        ))
        OR (exercise_movement_type IS NOT NULL AND exercise_movement_type <> regexp_replace(
            upper(substr(exercise_movement_type, 1, 1)) || lower(substr(exercise_movement_type, 2)),
            '\s+', ' ', 'g'
        ))
        OR (exercise_bodysplit IS NOT NULL AND exercise_bodysplit <> regexp_replace(
            upper(substr(exercise_bodysplit, 1, 1)) || lower(substr(exercise_bodysplit, 2)),
            '\s+', ' ', 'g'
        ));

    GET DIAGNOSTICS rows_updated_capitalize = ROW_COUNT;

    total_changed := rows_deleted + rows_updated_special_chars + rows_updated_capitalize;

    RAISE NOTICE '[cl_exercises] Rows deleted: %, (special_chars): %, (capitalize): %, Total changed: %',
                 rows_deleted, rows_updated_special_chars, rows_updated_capitalize, total_changed;

END;
$$;

CALL clean_exercises();
