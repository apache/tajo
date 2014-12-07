CREATE EXTERNAL TABLE ${0} (
  t_timestamp  TIMESTAMP,
  t_date    DATE
) USING TEXTFILE WITH ('timezone' = 'ASIA/Seoul') LOCATION ${table.path}