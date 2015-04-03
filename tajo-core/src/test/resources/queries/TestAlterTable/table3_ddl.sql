CREATE EXTERNAL TABLE ${0} (t_timestamp TIMESTAMP, t_date DATE) USING TEXT WITH('text.delimiter'='|', 'timezone'='ASIA/Seoul') LOCATION ${table.path};
