INSERT INTO core.dim_time (time_id, time, hour, minute, part_of_day)
SELECT
  t_id,
  t::time,
  EXTRACT(HOUR FROM t::time)::smallint,
  EXTRACT(MINUTE FROM t::time)::smallint,
  CASE
    WHEN EXTRACT(HOUR FROM t::time) >= 6 AND EXTRACT(HOUR FROM t::time) < 12 THEN 'morning'
    WHEN EXTRACT(HOUR FROM t::time) >= 12 AND EXTRACT(HOUR FROM t::time) < 18 THEN 'afternoon'
    WHEN EXTRACT(HOUR FROM t::time) >= 18 AND EXTRACT(HOUR FROM t::time) < 23 THEN 'evening'
    ELSE 'night'
  END AS part_of_day
FROM (
  SELECT
    generate_series(0, 1439) AS t_id,
    to_timestamp(generate_series(0, 1439) * 60) AS t
) s;