create external table table2 (id int, name text, value float) using csv
with ('csvfile.delimiter'=',') location ${table.path};