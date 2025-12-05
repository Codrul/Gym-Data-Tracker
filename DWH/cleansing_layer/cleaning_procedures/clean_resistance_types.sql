CREATE OR REPLACE PROCEDURE clean_resistance_types()
LANGUAGE plpgsql
AS $$
DECLARE
    rows_deleted               INT := 0;
    rows_updated_special_chars INT := 0;
    rows_updated_capitalize    INT := 0;
    total_changed              INT := 0;
BEGIN

    -- Deduplicate rows using normalized values
    WITH duplicates AS (
        SELECT
            resistance_src_id,
            COALESCE(
                regexp_replace(
                    upper(substr(TRIM(resistance_type), 1, 1)) || lower(substr(TRIM(resistance_type), 2)),
                    '\s+', ' ', 'g'
                ), 'N/A'
            ) AS resistance_type_norm,
            COALESCE(
                regexp_replace(
                    upper(substr(TRIM(resistance_category), 1, 1)) || lower(substr(TRIM(resistance_category), 2)),
                    '\s+', ' ', 'g'
                ), 'N/A'
            ) AS resistance_category_norm,
            MIN(resistance_id) AS min_id
        FROM cleansing_layer.cl_resistance_types
        GROUP BY resistance_src_id, resistance_type_norm, resistance_category_norm
        HAVING COUNT(*) > 1
    )
    DELETE FROM cleansing_layer.cl_resistance_types rt
    USING duplicates d
    WHERE rt.resistance_src_id = d.resistance_src_id
      AND COALESCE(
              regexp_replace(
                  upper(substr(TRIM(rt.resistance_type), 1, 1)) || lower(substr(TRIM(rt.resistance_type), 2)),
                  '\s+', ' ', 'g'
              ), 'N/A'
          ) = d.resistance_type_norm
      AND COALESCE(
              regexp_replace(
                  upper(substr(TRIM(rt.resistance_category), 1, 1)) || lower(substr(TRIM(rt.resistance_category), 2)),
                  '\s+', ' ', 'g'
              ), 'N/A'
          ) = d.resistance_category_norm
      AND rt.resistance_id <> d.min_id;

    GET DIAGNOSTICS rows_deleted = ROW_COUNT;

    -- Remove special characters
    UPDATE cleansing_layer.cl_resistance_types
    SET
        resistance_type     = regexp_replace(resistance_type, '[^0-9A-Za-z\s\-\.,]', '', 'g'),
        resistance_category = regexp_replace(resistance_category, '[^0-9A-Za-z\s\-\.,]', '', 'g')
    WHERE
        (resistance_type ~ '[^0-9A-Za-z\s\-\.,]' AND resistance_type != 'N/A')
        OR (resistance_category ~ '[^0-9A-Za-z\s\-\.,]' AND resistance_category != 'N/A');

    GET DIAGNOSTICS rows_updated_special_chars = ROW_COUNT;

    -- Trim spaces and capitalize first letter
    UPDATE cleansing_layer.cl_resistance_types
    SET
        resistance_type     = regexp_replace(
                                 upper(substr(resistance_type, 1, 1)) || lower(substr(resistance_type, 2)),
                                 '\s+', ' ', 'g'
                             ),
        resistance_category = regexp_replace(
                                 upper(substr(resistance_category, 1, 1)) || lower(substr(resistance_category, 2)),
                                 '\s+', ' ', 'g'
                             )
    WHERE
        (resistance_type != 'N/A' AND resistance_type <> regexp_replace(
            upper(substr(resistance_type, 1, 1)) || lower(substr(resistance_type, 2)),
            '\s+', ' ', 'g'
        ))
        OR (resistance_category != 'N/A' AND resistance_category <> regexp_replace(
            upper(substr(resistance_category, 1, 1)) || lower(substr(resistance_category, 2)),
            '\s+', ' ', 'g'
        ));

    GET DIAGNOSTICS rows_updated_capitalize = ROW_COUNT;

    total_changed := rows_deleted + rows_updated_special_chars + rows_updated_capitalize;

    RAISE NOTICE '[cl_resistance_types] Rows deleted: %, (special_chars): %, (capitalize): %, Total changed: %',
                 rows_deleted, rows_updated_special_chars, rows_updated_capitalize, total_changed;

END;
$$;

CALL clean_resistance_types();
