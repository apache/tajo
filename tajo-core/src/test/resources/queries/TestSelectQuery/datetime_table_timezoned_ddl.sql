CREATE EXTERNAL TABLE ${0} (
  t_timestamp  TIMESTAMP,
  t_date    DATE
) USING TEXT WITH ('timezone' = 'GMT+9') LOCATION ${table.path}
