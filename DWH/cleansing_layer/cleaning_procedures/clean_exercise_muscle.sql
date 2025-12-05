CREATE OR REPLACE PROCEDURE clean_exercise_muscle()
LANGUAGE plpgsql
AS $$
DECLARE
    rows_deleted               INT := 0;
    rows_updated_special_chars INT := 0;
    rows_updated_capitalize    INT := 0;
    total_changed              INT := 0;
BEGIN

    -- Deduplicate rows using normalized values (no exercise_muscle_id)
    WITH duplicates AS (
        SELECT
            exercise_src_id,
            COALESCE(
                regexp_replace(
                    upper(substr(TRIM(exercise_name), 1, 1)) || lower(substr(TRIM(exercise_name), 2)),
                    '\s+', ' ', 'g'
                ), 'N/A'
            ) AS exercise_name_norm,
            muscle_src_id,
            COALESCE(
                regexp_replace(
                    upper(substr(TRIM(muscle_name), 1, 1)) || lower(substr(TRIM(muscle_name), 2)),
                    '\s+', ' ', 'g'
                ), 'N/A'
            ) AS muscle_name_norm,
            COALESCE(
                regexp_replace(
                    upper(substr(TRIM(muscle_role), 1, 1)) || lower(substr(TRIM(muscle_role), 2)),
                    '\s+', ' ', 'g'
                ), 'N/A'
            ) AS muscle_role_norm,
            MIN(ctid) AS min_ctid
        FROM cleansing_layer.cl_exercise_muscle
        GROUP BY exercise_src_id, exercise_name_norm, muscle_src_id, muscle_name_norm, muscle_role_norm
        HAVING COUNT(*) > 1
    )
    DELETE FROM cleansing_layer.cl_exercise_muscle em
    USING duplicates d
    WHERE em.exercise_src_id = d.exercise_src_id
      AND COALESCE(
              regexp_replace(
                  upper(substr(TRIM(em.exercise_name), 1, 1)) || lower(substr(TRIM(em.exercise_name), 2)),
                  '\s+', ' ', 'g'
              ), 'N/A'
          ) = d.exercise_name_norm
      AND em.muscle_src_id = d.muscle_src_id
      AND COALESCE(
              regexp_replace(
                  upper(substr(TRIM(em.muscle_name), 1, 1)) || lower(substr(TRIM(em.muscle_name), 2)),
                  '\s+', ' ', 'g'
              ), 'N/A'
          ) = d.muscle_name_norm
      AND COALESCE(
              regexp_replace(
                  upper(substr(TRIM(em.muscle_role), 1, 1)) || lower(substr(TRIM(em.muscle_role), 2)),
                  '\s+', ' ', 'g'
              ), 'N/A'
          ) = d.muscle_role_norm
      AND em.ctid <> d.min_ctid; -- Use ctid to keep one row

    GET DIAGNOSTICS rows_deleted = ROW_COUNT;

    -- Remove special characters
    UPDATE cleansing_layer.cl_exercise_muscle
    SET
        exercise_name = regexp_replace(exercise_name, '[^0-9A-Za-z\s\-\.,]', '', 'g'),
        muscle_name   = regexp_replace(muscle_name, '[^0-9A-Za-z\s\-\.,]', '', 'g'),
        muscle_role   = regexp_replace(muscle_role, '[^0-9A-Za-z\s\-\.,]', '', 'g')
    WHERE
        (exercise_name ~ '[^0-9A-Za-z\s\-\.,]' AND exercise_name != 'N/A')
        OR (muscle_name ~ '[^0-9A-Za-z\s\-\.,]' AND muscle_name != 'N/A')
        OR (muscle_role ~ '[^0-9A-Za-z\s\-\.,]' AND muscle_role != 'N/A');

    GET DIAGNOSTICS rows_updated_special_chars = ROW_COUNT;

    -- Trim spaces and capitalize first letter
    UPDATE cleansing_layer.cl_exercise_muscle
    SET
        exercise_name = regexp_replace(
                            upper(substr(exercise_name, 1, 1)) || lower(substr(exercise_name, 2)),
                            '\s+', ' ', 'g'
                        ),
        muscle_name   = regexp_replace(
                            upper(substr(muscle_name, 1, 1)) || lower(substr(muscle_name, 2)),
                            '\s+', ' ', 'g'
                        ),
        muscle_role   = regexp_replace(
                            upper(substr(muscle_role, 1, 1)) || lower(substr(muscle_role, 2)),
                            '\s+', ' ', 'g'
                        )
    WHERE
        (exercise_name != 'N/A' AND exercise_name <> regexp_replace(
            upper(substr(exercise_name, 1, 1)) || lower(substr(exercise_name, 2)),
            '\s+', ' ', 'g'
        ))
        OR (muscle_name != 'N/A' AND muscle_name <> regexp_replace(
            upper(substr(muscle_name, 1, 1)) || lower(substr(muscle_name, 2)),
            '\s+', ' ', 'g'
        ))
        OR (muscle_role != 'N/A' AND muscle_role <> regexp_replace(
            upper(substr(muscle_role, 1, 1)) || lower(substr(muscle_role, 2)),
            '\s+', ' ', 'g'
        ));

    GET DIAGNOSTICS rows_updated_capitalize = ROW_COUNT;

    total_changed := rows_deleted + rows_updated_special_chars + rows_updated_capitalize;

    RAISE NOTICE '[cl_exercise_muscle] Rows deleted: %, (special_chars): %, (capitalize): %, Total changed: %',
                 rows_deleted, rows_updated_special_chars, rows_updated_capitalize, total_changed;

END;
$$;

CALL clean_exercise_muscle();
