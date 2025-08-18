CREATE OR REPLACE PROCEDURE master()
LANGUAGE plpgsql
AS $$
DECLARE
	v_rows_affected INT := 0;
	v_message_text TEXT;
	v_detail_text TEXT;
  v_hint_text TEXT;
	v_status_code INT;
BEGIN
  BEGIN
   	CALL load_exercises_to_cl();
   	CALL load_muscles_to_cl();
   	CALL load_resistances_to_cl();
	CALL load_exercise_muscle_to_cl();
   	CALL load_workouts_to_cl();
	
	-- cleaning 
	CALL clean_exercises();
	CALL clean_muscles();
	CALL clean_exercise_muscle();
	CALL clean_resistance_types();
	CALL clean_workouts();

 	-- will need to add a mapping procedure from the cleaning layer
		
	-- then load to 3nf
	CALL load_exercises_to_3nf();
	CALL load_muscles_to_3nf();
	CALL load_resistance_types_to_3nf();
	CALL load_exercise_muscle_to_3nf();
	CALL load_dates();
	CALL bl_3nf.manage_fact_workouts_partitions();
	CALL load_workouts_to_3nf();
	END;
END;
$$;

CALL master();

