CREATE SCHEMA IF NOT EXISTS bl_3nf;

-- now creating tables
CREATE TABLE IF NOT EXISTS bl_3nf.FACT_workouts (
    id BIGINT NOT NULL,
    src_id VARCHAR(10) NOT NULL,
    workout_number INT NOT NULL,
    date_id DATE NOT NULL,
    set_number INT NOT NULL,
    exercise_id INT NOT NULL, 
    reps NUMERIC NOT NULL,
    "load" NUMERIC NOT NULL,
    unit VARCHAR(3) NOT NULL,
    resistance_id BIGINT NOT NULL,
    set_type VARCHAR(20) NOT NULL,
    "comments" TEXT NOT NULL,
    workout_type VARCHAR(10) NOT NULL,
    TA_created_at TIMESTAMP NOT NULL,
    PRIMARY KEY(id, date_id, set_number, workout_number, exercise_id),
    UNIQUE(src_id, date_id, workout_number, set_number, exercise_id)
)
PARTITION BY RANGE (date_id);

-- creating partitions for my fact table 
CREATE TABLE IF NOT EXISTS bl_3nf.FACT_workouts_def PARTITION OF bl_3nf.FACT_workouts DEFAULT;

CREATE TABLE IF NOT EXISTS bl_3nf.FACT_workouts_june PARTITION OF bl_3nf.FACT_workouts 
    FOR VALUES FROM ('2025-06-01') TO ('2025-06-30');

CREATE TABLE IF NOT EXISTS bl_3nf.FACT_workouts_july PARTITION OF bl_3nf.FACT_workouts 
    FOR VALUES FROM ('2025-07-01') TO ('2025-07-31');

CREATE TABLE IF NOT EXISTS bl_3nf.FACT_workouts_august PARTITION OF bl_3nf.FACT_workouts 
    FOR VALUES FROM ('2025-08-01') TO ('2025-08-31');

CREATE TABLE IF NOT EXISTS bl_3nf.FACT_workouts_september PARTITION OF bl_3nf.FACT_workouts 
    FOR VALUES FROM ('2025-09-01') TO ('2025-09-30');

CREATE TABLE IF NOT EXISTS bl_3nf.FACT_workouts_october PARTITION OF bl_3nf.FACT_workouts 
    FOR VALUES FROM ('2025-10-01') TO ('2025-10-31');

CREATE TABLE IF NOT EXISTS bl_3nf.FACT_workouts_november PARTITION OF bl_3nf.FACT_workouts 
    FOR VALUES FROM ('2025-11-01') TO ('2025-11-30');

CREATE TABLE IF NOT EXISTS bl_3nf.FACT_workouts_december PARTITION OF bl_3nf.FACT_workouts 
    FOR VALUES FROM ('2025-12-01') TO ('2025-12-31');

-- back to creating the other tables 
CREATE TABLE IF NOT EXISTS bl_3nf.CE_exercises (
    exercise_id BIGINT UNIQUE NOT NULL,
    exercise_src_id VARCHAR(20) UNIQUE NOT NULL,
    exercise_name VARCHAR(256) NOT NULL,
    exercise_movement_type VARCHAR(256) NOT NULL,
    exercise_bodysplit VARCHAR(20) NOT NULL,
    TA_created_at TIMESTAMP,
    TA_updated_at TIMESTAMP,
    PRIMARY KEY(exercise_id) 
);

CREATE TABLE IF NOT EXISTS bl_3nf.CE_muscles (
    muscle_id BIGINT UNIQUE NOT NULL,
    muscle_src_id VARCHAR(20) UNIQUE NOT NULL,
    muscle_name VARCHAR(256) NOT NULL,
    muscle_group VARCHAR(256) NOT NULL,
    TA_created_at TIMESTAMP NOT NULL,
    TA_updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (muscle_id)
);

CREATE TABLE IF NOT EXISTS bl_3nf.CE_exercise_muscle (
    exercise_id BIGINT NOT NULL,
    exercise_src_id VARCHAR(20) NOT NULL,
    muscle_id BIGINT NOT NULL,
    muscle_src_id VARCHAR(20) NOT NULL,
    muscle_role VARCHAR(256) NOT NULL,
    TA_created_at TIMESTAMP NOT NULL,
    TA_updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY(exercise_id, muscle_id),
    UNIQUE(exercise_src_id, muscle_src_id)
);

CREATE TABLE IF NOT EXISTS bl_3nf.CE_resistance_types (
    resistance_id BIGINT UNIQUE NOT NULL,
    resistance_src_id VARCHAR(20) UNIQUE NOT NULL,
    resistance_type VARCHAR(256) NOT NULL,
    resistance_category VARCHAR(256) NOT NULL,
    TA_created_at TIMESTAMP NOT NULL,
    TA_updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY(resistance_id)
);

CREATE TABLE IF NOT EXISTS bl_3nf.CE_Dates (
    "DATE" DATE PRIMARY KEY,
    DAY_NAME_IN_WEEK VARCHAR(15),
    DAY_NUMBER_IN_MONTH INT,
    CALENDAR_MONTH_NUMBER INT,
    CALENDAR_MONTH_DESC VARCHAR(15),
    CALENDAR_YEAR INT,
    IS_WEEKEND VARCHAR(1)
);

-- now onto creating the indexes
CREATE UNIQUE INDEX IF NOT EXISTS idx_CE_resistance_types_src_id
    ON bl_3nf.CE_resistance_types (resistance_src_id);
   
CREATE UNIQUE INDEX IF NOT EXISTS idx_bl_exercise_src_id
    ON bl_3nf.CE_exercises (exercise_src_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_CE_muscles_src_id
    ON bl_3nf.CE_muscles (muscle_src_id);
   
CREATE UNIQUE INDEX IF NOT EXISTS idx_CE_exercise_muscle_src_ids
    ON bl_3nf.CE_exercise_muscle (exercise_src_id, muscle_src_id);
   
CREATE UNIQUE INDEX IF NOT EXISTS idx_FACT_workouts_src_id_date_workout_set_exercise
    ON bl_3nf.FACT_workouts (src_id, date_id, workout_number, set_number, exercise_id);

-- now to create the relationships 
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint 
                   WHERE conname = 'fk_workouts_exercise' 
                     AND conrelid = 'bl_3nf.FACT_workouts'::regclass) THEN
        ALTER TABLE bl_3nf.FACT_workouts
        ADD CONSTRAINT fk_workouts_exercise
        FOREIGN KEY (exercise_id) REFERENCES bl_3nf.CE_exercises(exercise_id);
    ELSE
        RAISE NOTICE 'Foreign key fk_workouts_exercise already exists on FACT_workouts.';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint 
                   WHERE conname = 'fk_exercise_muscle_exercise' 
                     AND conrelid = 'bl_3nf.CE_exercise_muscle'::regclass) THEN
        ALTER TABLE bl_3nf.CE_exercise_muscle
        ADD CONSTRAINT fk_exercise_muscle_exercise
        FOREIGN KEY (exercise_id) REFERENCES bl_3nf.CE_exercises(exercise_id);
    ELSE
        RAISE NOTICE 'Foreign key fk_exercise_muscle_exercise already exists on CE_exercise_muscle.';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint 
                   WHERE conname = 'fk_exercise_muscle_muscle' 
                     AND conrelid = 'bl_3nf.CE_exercise_muscle'::regclass) THEN
        ALTER TABLE bl_3nf.CE_exercise_muscle
        ADD CONSTRAINT fk_exercise_muscle_muscle
        FOREIGN KEY (muscle_id) REFERENCES bl_3nf.CE_muscles(muscle_id);
    ELSE
        RAISE NOTICE 'Foreign key fk_exercise_muscle_muscle already exists on CE_exercise_muscle.';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint 
                   WHERE conname = 'fk_workouts_resistance' 
                     AND conrelid = 'bl_3nf.FACT_workouts'::regclass) THEN
        ALTER TABLE bl_3nf.FACT_workouts
        ADD CONSTRAINT fk_workouts_resistance
        FOREIGN KEY (resistance_id) REFERENCES bl_3nf.CE_resistance_types(resistance_id);
    ELSE
        RAISE NOTICE 'Foreign key fk_workouts_resistance already exists on FACT_workouts.';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint 
                   WHERE conname = 'fk_workouts_dates' 
                     AND conrelid = 'bl_3nf.FACT_workouts'::regclass) THEN
        ALTER TABLE bl_3nf.FACT_workouts
        ADD CONSTRAINT fk_workouts_dates
        FOREIGN KEY (date_id) REFERENCES bl_3nf.CE_Dates("DATE");
    ELSE
        RAISE NOTICE 'Foreign key fk_workouts_dates already exists on FACT_workouts.';
    END IF;
END $$;

-- now generating sequences
CREATE SEQUENCE IF NOT EXISTS bl_3nf.workout_id_seq
START WITH 1
INCREMENT BY 1;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.exercise_id_seq
START WITH 1
INCREMENT BY 1;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.muscle_id_seq
START WITH 1
INCREMENT BY 1;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.resistance_id_seq
START WITH 1
INCREMENT BY 1;
