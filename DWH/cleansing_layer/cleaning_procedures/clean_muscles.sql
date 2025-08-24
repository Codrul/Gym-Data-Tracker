CREATE OR REPLACE PROCEDURE clean_muscles()
LANGUAGE plpgsql
AS $$
DECLARE
    rows_deleted               INT := 0;
    rows_updated_special_chars INT := 0;
    rows_updated_capitalize    INT := 0;
    total_changed              INT := 0;
BEGIN

    WITH duplicates AS (
        SELECT
            muscle_src_id,
            COALESCE(
                regexp_replace(
                    upper(substr(TRIM(muscle_name), 1, 1)) || lower(substr(TRIM(muscle_name), 2)),
                    '\s+', ' ', 'g'
                ), 'N/A'
            ) AS muscle_name_norm,
            COALESCE(
                regexp_replace(
                    upper(substr(TRIM(muscle_group), 1, 1)) || lower(substr(TRIM(muscle_group), 2)),
                    '\s+', ' ', 'g'
                ), 'N/A'
            ) AS muscle_group_norm,
            MIN(muscle_id) AS min_id
        FROM cleansing_layer.cl_muscles
        GROUP BY muscle_src_id, muscle_name_norm, muscle_group_norm
        HAVING COUNT(*) > 1
    )
    DELETE FROM cleansing_layer.cl_muscles m
    USING duplicates d
    WHERE m.muscle_src_id = d.muscle_src_id
      AND COALESCE(
              regexp_replace(
                  upper(substr(TRIM(m.muscle_name), 1, 1)) || lower(substr(TRIM(m.muscle_name), 2)),
                  '\s+', ' ', 'g'
              ), 'N/A'
          ) = d.muscle_name_norm
      AND COALESCE(
              regexp_replace(
                  upper(substr(TRIM(m.muscle_group), 1, 1)) || lower(substr(TRIM(m.muscle_group), 2)),
                  '\s+', ' ', 'g'
              ), 'N/A'
          ) = d.muscle_group_norm
      AND m.muscle_id <> d.min_id;

    GET DIAGNOSTICS rows_deleted = ROW_COUNT;

    UPDATE cleansing_layer.cl_muscles
    SET
        muscle_name  = regexp_replace(muscle_name, '[^0-9A-Za-z\s\-\.,]', '', 'g'),
        muscle_group = regexp_replace(muscle_group, '[^0-9A-Za-z\s\-\.,]', '', 'g')
    WHERE
        (muscle_name ~ '[^0-9A-Za-z\s\-\.,]' AND muscle_name != 'N/A')
        OR (muscle_group ~ '[^0-9A-Za-z\s\-\.,]' AND muscle_group != 'N/A');

    GET DIAGNOSTICS rows_updated_special_chars = ROW_COUNT;

    UPDATE cleansing_layer.cl_muscles
    SET
        muscle_name  = regexp_replace(
                            upper(substr(muscle_name, 1, 1)) || lower(substr(muscle_name, 2)),
                            '\s+', ' ', 'g'
                        ),
        muscle_group = regexp_replace(
                            upper(substr(muscle_group, 1, 1)) || lower(substr(muscle_group, 2)),
                            '\s+', ' ', 'g'
                        )
    WHERE
        (muscle_name != 'N/A' AND muscle_name <> regexp_replace(
            upper(substr(muscle_name, 1, 1)) || lower(substr(muscle_name, 2)),
            '\s+', ' ', 'g'
        ))
        OR (muscle_group != 'N/A' AND muscle_group <> regexp_replace(
            upper(substr(muscle_group, 1, 1)) || lower(substr(muscle_group, 2)),
            '\s+', ' ', 'g'
        ));

    GET DIAGNOSTICS rows_updated_capitalize = ROW_COUNT;

    total_changed := rows_deleted + rows_updated_special_chars + rows_updated_capitalize;

    RAISE NOTICE '[cl_muscles] Rows deleted: %, (special_chars): %, (capitalize): %, Total changed: %',
                 rows_deleted, rows_updated_special_chars, rows_updated_capitalize, total_changed;

END;
$$;

CALL clean_muscles();
