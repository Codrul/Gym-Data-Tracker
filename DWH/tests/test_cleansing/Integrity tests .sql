-- Invalid exercise_id
SELECT w.exercise_id
FROM cleansing_layer.cl_workouts w
LEFT JOIN cleansing_layer.cl_exercises e
       ON w.exercise_id = e.exercise_id
WHERE w.exercise_id IS NOT NULL
  AND e.exercise_id IS NULL;

-- Invalid resistance_id
SELECT w.resistance_id
FROM cleansing_layer.cl_workouts w
LEFT JOIN cleansing_layer.cl_resistance_types r
       ON w.resistance_id = r.resistance_id
WHERE w.resistance_id IS NOT NULL
  AND r.resistance_id IS NULL;
