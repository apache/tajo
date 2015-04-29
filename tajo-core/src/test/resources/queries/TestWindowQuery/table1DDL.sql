create external table ${0} (id int, name text, num int) using csv
with ('csvfile.delimiter'='|', 'csvfile.null'='NULL') location ${table.path};