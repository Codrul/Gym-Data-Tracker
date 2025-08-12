UPDATE cleansing_layer.cl_workouts
SET
  unit = CASE
           WHEN "load" ~* 'kg' THEN 'KG'
           WHEN "load" ~* 'lbs' THEN 'LBS'
           ELSE 'N/A'
         END,
  "load" = regexp_replace("load", '[^0-9.]', '', 'g')
WHERE "load" ~* '(kg|lbs)';

