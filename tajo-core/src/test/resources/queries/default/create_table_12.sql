create external table table1 (name text, age int)
USING text WITH ('text.delimiter'='|')
location '/user/hive/table1'