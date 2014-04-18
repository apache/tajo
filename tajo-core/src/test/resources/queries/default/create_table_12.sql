create external table table1 (name text, age int)
USING csv WITH ('csvfile.delimiter'='|')
location '/user/hive/table1'