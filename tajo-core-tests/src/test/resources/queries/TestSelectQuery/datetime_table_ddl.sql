CREATE EXTERNAL TABLE ${0} (
  t_timestamp  TIMESTAMP,
  t_time       TIME,
  t_date       DATE
) USING TEXT LOCATION ${table.path}
