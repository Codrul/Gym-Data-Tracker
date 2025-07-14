CREATE SCHEMA IF NOT EXISTS staging_layer;

CREATE TABLE IF NOT EXISTS staging_layer.exercises(
	exercise_id VARCHAR(256),
	exercise_name VARCHAR(256),
	exercise_movement_type VARCHAR(256),
	exercise_bodysplit VARCHAR(256)
);

CREATE TABLE IF NOT EXISTS staging_layer.muscles(
	muscle_id VARCHAR(256),
	muscle_name VARCHAR(256),
	muscle_group VARCHAR(256)
);

CREATE TABLE IF NOT EXISTS staging_layer.resistance_types(
	resistance_id VARCHAR(256),
	resistance_type VARCHAR(256),
	resistance_category VARCHAR(256)
);

CREATE TABLE IF NOT EXISTS staging_layer.exercise_muscle (
	exercise_id VARCHAR(256),
	exercise_name VARCHAR(256),
	muscle_id VARCHAR(256),
	muscle_name VARCHAR(256),
	muscle_role VARCHAR(256)
);
 -- will need to create the fact table after I think it through 
