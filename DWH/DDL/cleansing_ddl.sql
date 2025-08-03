CREATE SCHEMA IF NOT EXISTS cleansing_layer; 

CREATE TABLE IF NOT EXISTS cleansing_layer.cl_exercises(
	exercise_id VARCHAR(256),
	exercise_src_id VARCHAR(256),
	exercise_name VARCHAR(256),
	exercise_movement_type VARCHAR(256),
	exercise_bodysplit VARCHAR(256),
  	created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cleansing_layer.cl_muscles(
	muscle_id VARCHAR(256),
	muscle_src_id VARCHAR(256),
	muscle_name VARCHAR(256),
	muscle_group VARCHAR(256),
 	created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cleansing_layer.cl_resistance_types(
	resistance_id VARCHAR(256),
	resistance_src_id VARCHAR(256),
	resistance_type VARCHAR(256),
	resistance_category VARCHAR(256),
  	created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cleansing_layer.cl_exercise_muscle(
	exercise_id VARCHAR(256),
	exercise_src_id VARCHAR(256),
	exercise_name VARCHAR(256),
	muscle_id VARCHAR(256),
	muscle_src_id VARCHAR(256),
	muscle_name VARCHAR(256),
	muscle_role VARCHAR(256),
  	created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cleansing_layer.cl_workouts(
	workout_id VARCHAR(256),
	workout_src_id VARCHAR(256),
	"date" VARCHAR(256),
	set_number VARCHAR(256),
	exercise VARCHAR(256),
	reps VARCHAR(256),
	"load" VARCHAR(256),
	"unit" VARCHAR(256),
	resistance_type VARCHAR(256),
	set_type VARCHAR(256),
	"comments" VARCHAR(256), 
	workout_type VARCHAR(256),
  	created_at TIMESTAMP
);

-- now creating some ID sequences
CREATE SEQUENCE IF NOT EXISTS cleansing_layer.exercise_id_seq
START WITH 1 
INCREMENT BY 1;

CREATE SEQUENCE IF NOT EXISTS cleansing_layer.muscle_id_seq
START WITH 1 
INCREMENT BY 1;

CREATE SEQUENCE IF NOT EXISTS cleansing_layer.workout_id_seq
START WITH 1 
INCREMENT BY 1;

CREATE SEQUENCE IF NOT EXISTS cleansing_layer.resistance_id_seq
START WITH 1 
INCREMENT BY 1;