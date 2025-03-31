INSERT INTO core.dim_date (
  date_id, date, year, month_num, month_abbr, month_name, day, day_name, week, is_weekend
)
SELECT
  EXTRACT(YEAR FROM d)::int * 10000 +
  EXTRACT(MONTH FROM d)::int * 100 +
  EXTRACT(DAY FROM d)::int AS date_id,
  d::date AS date,
  EXTRACT(YEAR FROM d)::smallint AS year,
  EXTRACT(MONTH FROM d)::smallint AS month_num,
  to_char(d, 'Mon') AS month_abbr,
  to_char(d, 'FMMonth') AS month_name,
  EXTRACT(DAY FROM d)::smallint AS day,
  to_char(d, 'Day') AS day_name,
  EXTRACT(WEEK FROM d)::smallint AS week,
  CASE WHEN EXTRACT(ISODOW FROM d) IN (6,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM generate_series('2018-01-01'::timestamp, '2030-12-31'::timestamp, interval '1 day') AS d;
