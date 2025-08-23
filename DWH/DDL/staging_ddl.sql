CREATE SCHEMA IF NOT EXISTS staging_layer;

CREATE TABLE IF NOT EXISTS staging_layer.exercises(
	exercise_id VARCHAR(256),
	exercise_name VARCHAR(256),
	exercise_movement_type VARCHAR(256),
	exercise_bodysplit VARCHAR(256),
  	created_at TIMESTAMP,
  	updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging_layer.muscles(
	muscle_id VARCHAR(256),
	muscle_name VARCHAR(256),
	muscle_group VARCHAR(256),
  	created_at TIMESTAMP,
  	updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging_layer.resistance_types(
	resistance_id VARCHAR(256),
	resistance_type VARCHAR(256),
	resistance_category VARCHAR(256),
  	created_at TIMESTAMP,
  	updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging_layer.exercise_muscle (
	exercise_id VARCHAR(256),
	exercise_name VARCHAR(256),
	muscle_id VARCHAR(256),
	muscle_name VARCHAR(256),
	muscle_role VARCHAR(256),
  	created_at TIMESTAMP,
  	updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging_layer.workouts(
	workout_number VARCHAR(256),
	"date" VARCHAR(256),
	set_number VARCHAR(256),
	exercise VARCHAR(256),
	reps VARCHAR(256),
	"load" VARCHAR(256),
	resistance_type VARCHAR(256),
	set_type VARCHAR(256),
	"comments" VARCHAR(256),
	workout_type VARCHAR(256),
  	created_at TIMESTAMP,
  	updated_at TIMESTAMP
);
